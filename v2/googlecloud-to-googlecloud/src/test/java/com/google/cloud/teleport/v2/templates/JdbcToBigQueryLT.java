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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManagerUtils.toTableSpec;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.datagenerator.BacklogGenerator;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager.JDBCSchema;
import com.google.cloud.teleport.it.jdbc.PostgresResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link JdbcToBigQuery Jdbc To BigQuery} Flex template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(JdbcToBigQuery.class)
@RunWith(JUnit4.class)
public class JdbcToBigQueryLT extends TemplateLoadTestBase {
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = JdbcToBigQueryLT.class.getSimpleName().toLowerCase();
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Jdbc_to_BigQuery_Flex");
  private static final Schema BQ_SCHEMA =
      Schema.of(
          Field.of("eventId", StandardSQLTypeName.STRING),
          Field.of("eventTimestamp", StandardSQLTypeName.STRING),
          Field.of("ipv4", StandardSQLTypeName.STRING),
          Field.of("ipv6", StandardSQLTypeName.STRING),
          Field.of("country", StandardSQLTypeName.STRING),
          Field.of("username", StandardSQLTypeName.STRING),
          Field.of("quest", StandardSQLTypeName.STRING),
          Field.of("score", StandardSQLTypeName.INT64),
          Field.of("completed", StandardSQLTypeName.BOOL));
  private static final JDBCSchema JDBC_SCHEMA =
      new JDBCSchema(
          Map.of(
              "eventId", "VARCHAR(100)",
              "eventTimestamp", "TIMESTAMP",
              "ipv4", "VARCHAR(100)",
              "ipv6", "VARCHAR(100)",
              "country", "VARCHAR(100)",
              "username", "VARCHAR(100)",
              "quest", "VARCHAR(100)",
              "score", "INTEGER",
              "completed", "BOOLEAN"),
          "eventId");
  private static final String STATEMENT =
      "INSERT INTO %s (eventId,eventTimestamp,ipv4,ipv6,country,username,quest,score,completed) VALUES (?,to_timestamp(?/1000),?,?,?,?,?,?,?)";
  private static final String QUERY = "select * from %s";
  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";
  private static final String INPUT_PCOLLECTION = "Read from JdbcIO/ParDo(DynamicRead).out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to BigQuery/PrepareWrite/ParDo(Anonymous).out0";
  private static JDBCResourceManager jdbcResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    jdbcResourceManager = PostgresResourceManager.builder(testName).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project).setCredentials(CREDENTIALS).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(jdbcResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(BacklogConfiguration.of(10_000_000L, 30, 40), this::disableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog(BacklogConfiguration.of(10_000_000L, 30, 40), this::enableRunnerV2);
  }

  @Ignore(
      "Storage API in batch mode leads to data loss. https://github.com/apache/beam/issues/26521")
  @Test
  public void testBacklog10gbUsingStorageApi()
      throws IOException, ParseException, InterruptedException {
    testBacklog(
        BacklogConfiguration.of(10_000_000L, 30, 40),
        config ->
            config
                .addParameter("useStorageWriteApi", "true")
                .addParameter("experiments", "disable_runner_v2"));
  }

  @Ignore
  @Test
  public void testBacklog100gb() throws IOException, ParseException, InterruptedException {
    // 350,000,000 messages of the given schema make up approximately 100GB
    testBacklog(BacklogConfiguration.of(100_000_000L, 60, 120), this::disableRunnerV2);
  }

  public void testBacklog(
      BacklogConfiguration configuration,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    jdbcResourceManager.createTable(testName, new JDBCSchema(Map.of("bytes", "bytea"), "bytes"));
    TableId table =
        bigQueryResourceManager.createTable(
            testName, Schema.of(Field.of("bytes", StandardSQLTypeName.STRING)));
    // Generate fake data to table
    BacklogGenerator dataGenerator =
        BacklogGenerator.builder(testName)
            .setSinkType("JDBC")
            .setRowSize(configuration.getRowSize())
            .setNumRows(configuration.getNumRows())
            .setDriverClassName(DRIVER_CLASS_NAME)
            .setConnectionUrl(jdbcResourceManager.getUri())
            .setStatement(
                String.format(
                    "INSERT INTO %s values (?)", testName)) // String.format(STATEMENT, testName))
            .setUsername(jdbcResourceManager.getUsername())
            .setPassword(jdbcResourceManager.getPassword())
            .build();
    dataGenerator.execute(Duration.ofMinutes(configuration.getGeneratorTimeout()));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addEnvironment(
                        "additionalUserLabels",
                        Collections.singletonMap("row-size", configuration.getRowSize() + "bytes"))
                    // Use a worker with high memory as this template uses DynamicJdbcIO,
                    // which does not parallelize reads
                    .addParameter("workerMachineType", "n1-highmem-8")
                    .addParameter(
                        "driverJars", "gs://apache-beam-pranavbhandari/postgresql-42.2.27.jar")
                    .addParameter("driverClassName", DRIVER_CLASS_NAME)
                    .addParameter("username", jdbcResourceManager.getUsername())
                    .addParameter("password", jdbcResourceManager.getPassword())
                    .addParameter("connectionURL", jdbcResourceManager.getUri())
                    .addParameter("query", String.format(QUERY, testName))
                    .addParameter("outputTable", toTableSpec(project, table))
                    .addParameter("bigQueryLoadingTemporaryDirectory", getTempDirectory()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result =
        pipelineOperator.waitUntilDone(
            createConfig(info, Duration.ofMinutes(configuration.getPipelineTimeout())));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(bigQueryResourceManager.getRowCount(table.getTable()))
        .isAtLeast(configuration.getNumRows());

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTempDirectory() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, testName, "temp");
  }
}
