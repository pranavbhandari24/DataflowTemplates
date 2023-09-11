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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.LoadTestBase;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.dataflow.FlexTemplateClient;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link SpannerChangeStreamsToGcs Spanner CDC to Gcs} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerChangeStreamsToGcs.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToGcsLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Spanner_Change_Streams_to_Google_Cloud_Storage");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      SpannerChangeStreamsToGcsLT.class.getSimpleName().toLowerCase();

  private static final String INPUT_PCOLLECTION =
      "SpannerIO.ReadChangeStream/Gather metrics/ParMultiDo(PostProcessingMetrics).out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write To GCS/Write Text File(s)/Transform to JSON Text/Map/ParMultiDo(Anonymous).out0";
  private static final Pattern EXPECTED_PATTERN = Pattern.compile(".*result-.*");

  private static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;

  @Before
  public void setup() {
    // Set up resource managers
    spannerResourceManager = SpannerResourceManager.builder(testName, project, region).build();
    gcsResourceManager =
        GcsResourceManager.builder(ARTIFACT_BUCKET, TEST_ROOT_DIR, CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, gcsResourceManager);
  }

  @Test
  public void testSteadyState1hr() throws IOException, ParseException, InterruptedException {
    testSteadyState1hr(Function.identity());
  }

  public void testSteadyState1hr(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    // QPS is set to 25K currently since DataGenerator is not able to handle writes at higher QPS
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
                + ") PRIMARY KEY(eventId, eventTimestamp)",
            testName);
    // creating spanner instance with 15 nodes.
    spannerResourceManager.createInstance(15);
    spannerResourceManager.executeDdlStatement(createTableStatement);
    // create spanner change stream
    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS(qps)
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(testName)
            // .setSpannerMaxNumRows("2000")
            // .setSpannerMaxNumMutations("10000")
            // .setSpannerBatchSizeBytes("2000000")
            .setSpannerGroupingFactor("1000")
            // .setSpannerCommitDeadlineSeconds("20")
            .setNumWorkers("45")
            .setMaxNumWorkers("45")
            .build();

    PipelineLauncher.LaunchConfig options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(testName, SPEC_PATH)
                    // pipeline needs runner v2
                    .addEnvironment(
                        "additionalExperiments", List.of("use_runner_v2", "min_num_workers=5"))
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
                    .addParameter("gcsOutputDirectory", getTestMethodDirPath())
                    .addParameter("windowDuration", "10s")
                    .addParameter("outputFilenamePrefix", "result-")
                    .addParameter("outputFileFormat", "TEXT"))
            .build();

    // Act
    PipelineLauncher.LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    // ElementCount metric in dataflow is approximate, allow for 1% difference
    long expectedMessages = (long) (dataGenerator.execute(Duration.ofMinutes(60)) * 0.99);
    PipelineOperator.Result result =
        pipelineOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(20)),
            () -> waitForNumMessages(info.jobId(), INPUT_PCOLLECTION, expectedMessages));
    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(gcsResourceManager.listArtifacts(testName, EXPECTED_PATTERN)).isNotEmpty();

    // export results
    // TODO: remove the below comment
    // exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsResourceManager.runId(), testName);
  }
}
