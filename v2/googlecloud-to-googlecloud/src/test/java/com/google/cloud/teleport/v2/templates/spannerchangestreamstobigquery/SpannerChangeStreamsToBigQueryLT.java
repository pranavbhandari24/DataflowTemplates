/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link SpannerChangeStreamsToBigQuery Spanner CDC to BigQuery} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToBigQueryLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Spanner_Change_Streams_to_BigQuery");

  private static final String INPUT_PCOLLECTION = null;
  private static final String OUTPUT_PCOLLECTION = null;
  private static SpannerResourceManager spannerResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    // Set up resource managers
    spannerResourceManager = SpannerResourceManager.builder(testName, project, region).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(Function.identity());
  }

  public void testSteadyState1hr(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    String qps = getProperty("qps", "100000", TestProperties.Type.PROPERTY);
    // create spanner table
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  eventId STRING(1024) NOT NULL,\n"
                + "  eventTimestamp INT64,\n"
                + "  ipv4 STRING(1024),\n"
                + "  ipv6 STRING(1024),\n"
                + "  country STRING(1024),\n"
                + "  username STRING(1024),\n"
                + "  quest STRING(1024),\n"
                + "  score INT64,\n"
                + "  completed BOOL,\n"
                + ") PRIMARY KEY(eventId)",
            testName);
    // creating spanner instance with 15 nodes.
    spannerResourceManager.createInstance(15);
    spannerResourceManager.executeDdlStatement(createTableStatement);
    // create spanner change stream
    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    // create bigquery dataset
    bigQueryResourceManager.createDataset(region);
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS(qps)
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(testName)
            .setSpannerMaxNumRows("2000")
            .setSpannerMaxNumMutations("10000")
            .setSpannerBatchSizeBytes("2000000")
            .setSpannerCommitDeadlineSeconds("20")
            .setNumWorkers("45")
            .setMaxNumWorkers("45")
            .build();

    // note: the bigquery tables are created by the pipeline automatically if they don't exist
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    // pipeline needs runner v2
                    .addEnvironment(
                        "additionalExperiments", Collections.singletonList("use_runner_v2"))
                    .addEnvironment("maxWorkers", 15)
                    .addEnvironment("numWorkers", 10)
                    .addEnvironment("additionalUserLabels", Collections.singletonMap("qps", qps))
                    .addParameter("spannerProjectId", project)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter(
                        "spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerChangeStreamName", testName + "_stream")
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    int expectedMessages = (int) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    TableId expectedTableId =
        TableId.of(project, bigQueryResourceManager.getDatasetId(), testName + "_changelog");
    Result result =
        pipelineOperator.waitForConditionAndCancel(
            createConfig(info, Duration.ofMinutes(60)),
            BigQueryRowsCheck.builder(bigQueryResourceManager, expectedTableId)
                .setMinRows(expectedMessages)
                .build());

    // Assert
    assertThatResult(result).meetsConditions();

    Map<String, Double> metrics = getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION);
    // Query end to end latency metrics from BigQuery
    TableResult latencyResult =
        bigQueryResourceManager.runQuery(
            String.format(
                "WITH difference AS (SELECT\n"
                    + "    TIMESTAMP_DIFF(_metadata_big_query_commit_timestamp,\n"
                    + "    TIMESTAMP_MILLIS(eventTimestamp), SECOND) AS latency,\n"
                    + "    FROM %s.%s)\n"
                    + "    SELECT\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.5) OVER () AS median,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.9) OVER () as percentile_90,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.95) OVER () as percentile_95,\n"
                    + "      PERCENTILE_CONT(difference.latency, 0.99) OVER () as percentile_99\n"
                    + "    FROM difference LIMIT 1",
                bigQueryResourceManager.getDatasetId(), expectedTableId.getTable()));

    FieldValueList latencyValues = latencyResult.getValues().iterator().next();
    metrics.put("median_latency", latencyValues.get(0).getDoubleValue());
    metrics.put("percentile_90_latency", latencyValues.get(1).getDoubleValue());
    metrics.put("percentile_95_latency", latencyValues.get(2).getDoubleValue());
    metrics.put("percentile_99_latency", latencyValues.get(3).getDoubleValue());

    // export results
    // TODO: remove the below comment
    // exportMetricsToBigQuery(info, metrics);
  }
}
