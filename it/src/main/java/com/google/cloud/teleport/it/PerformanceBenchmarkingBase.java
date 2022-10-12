/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it;

import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DefaultDataflowClient;
import com.google.cloud.teleport.it.monitoring.MonitoringClient;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for testing performance of dataflow templates. */
@RunWith(JUnit4.class)
public class PerformanceBenchmarkingBase {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataflowClient.class);
  // Dataflow resources cost factors (showing us-central-1 pricing).
  // See https://cloud.google.com/dataflow/pricing#pricing-details
  private static final double VCPU_PER_HR_BATCH = 0.056;
  private static final double VCPU_PER_HR_STREAMING = 0.069;
  private static final double MEM_PER_GB_HR_BATCH = 0.003557;
  private static final double MEM_PER_GB_HR_STREAMING = 0.0035557;
  private static final double PD_PER_GB_HR = 0.000054;
  private static final double PD_SSD_PER_GB_HR = 0.000298;
  private static final double SHUFFLE_PER_GB_BATCH = 0.011;
  private static final double SHUFFLE_PER_GB_STREAMING = 0.018;
  protected static final String PROJECT = TestProperties.project();
  protected static final String REGION = TestProperties.region();
  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);
  protected MonitoringClient monitoringClient;
  protected DataflowClient dataflowClient;
  protected DataflowOperator dataflowOperator;

  @Rule
  public final TestName testName = new TestName();

  @Before
  public void setUpBase() throws IOException {
    monitoringClient =
        MonitoringClient.builder().setCredentialsProvider(CREDENTIALS_PROVIDER).build();
    dataflowClient = DefaultDataflowClient.builder().setCredentials(CREDENTIALS).build();
    dataflowOperator = new DataflowOperator(dataflowClient);
  }

  @After
  public void tearDownBase() {
    monitoringClient.cleanupAll();
  }

  /**
   * Exports the metrics of given dataflow job to BigQuery.
   *
   * @param jobInfo Job info of the job
   * @param testName test name
   * @param outputPtransform output ptransform of the dataflow job to query additional metrics
   * @throws IOException
   * @throws ParseException
   * @throws InterruptedException
   */
  public void exportMetricsToBigQuery(JobInfo jobInfo, String testName, String outputPtransform)
      throws IOException, ParseException, InterruptedException {
    // Get metrics of the job
    BigQueryResourceManager bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setDatasetId(TestProperties.exportDataset())
            .setCredentials(CREDENTIALS)
            .build();
    Map<String, Double> metrics = getMetrics(jobInfo, outputPtransform);
    // exporting metrics to bigQuery table
    Map<String, Object> rowContent = new HashMap<>();
    rowContent.put("timestamp", jobInfo.startTime());
    rowContent.put("sdk", jobInfo.sdk());
    rowContent.put("version", jobInfo.version());
    rowContent.put("job_type", jobInfo.jobType());
    rowContent.put("template_name", jobInfo.templateName());
    rowContent.put("template_version", jobInfo.templateVersion());
    rowContent.put("template_type", jobInfo.templateType());
    rowContent.put("test_name", testName);
    // Convert parameters map to list of table row since it's a repeated record
    List<TableRow> parameterRows = new ArrayList<>();
    for (String parameter: jobInfo.parameters().keySet()) {
      TableRow row = new TableRow()
          .set("name", parameter)
          .set("value", jobInfo.parameters().get(parameter));
      parameterRows.add(row);
    }
    rowContent.put("parameters", parameterRows);
    // Convert metrics map to list of table row since it's a repeated record
    List<TableRow> metricRows = new ArrayList<>();
    for (String metric: metrics.keySet()) {
      TableRow row = new TableRow()
          .set("name", metric)
          .set("value", metrics.get(metric));
      metricRows.add(row);
    }
    rowContent.put("metrics", metricRows);
    bigQueryResourceManager.write(TestProperties.exportTable(), RowToInsert.of("rowId", rowContent));
  }

  /**
   * Exports the metrics of given dataflow job to Postgres DB.
   * 
   * @param jobInfo Job info of the job
   * @param testName test name
   * @param outputPtransform output ptransform of the dataflow job to query additional metrics
   * @throws IOException
   * @throws ParseException
   * @throws InterruptedException
   */
  public void exportMetricsToPostgres(JobInfo jobInfo, String testName, String outputPtransform)
      throws IOException, ParseException, InterruptedException {
    Map<String, Double> metrics = getMetrics(jobInfo, outputPtransform);
    Connection c = null;
    Statement stmt = null;
    try {
      Class.forName("org.postgresql.Driver");
      c = DriverManager
          .getConnection(TestProperties.connectionUrl(),
              TestProperties.username(), TestProperties.password());
      c.setAutoCommit(false);
      LOG.info("Opened connection to database successfully.");
      stmt = c.createStatement();
      String testId = "!";
      // Common table:
      // test_id, test name, timestamp, sdk, version, runner, job type
      String query =
          "INSERT INTO COMPANY (TEST_ID, TEST_NAME, TIMESTAMP, SDK, VERSION, RUNNER, JOB_TYPE) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?);";
      PreparedStatement pStmt = c.prepareStatement(query);
      pStmt.setString(1, testId);
      pStmt.setString(2, testName);
      pStmt.setTimestamp(3, Timestamp.valueOf(jobInfo.startTime()));
      pStmt.setString(4, jobInfo.sdk());
      pStmt.setString(5, jobInfo.version());
      pStmt.setString(6, jobInfo.runner());
      pStmt.setString(7, jobInfo.jobType());
      pStmt.execute();
      // Template table:
      // test_id, template name, template version, template type
      if (jobInfo.templateName() != null) {
        query = String.format("INSERT INTO COMPANY (TEST_ID, NAME, VERSION, TYPE) "
            + "VALUES (%s, %s, '%s', '%s');",
            testId, jobInfo.templateName(), jobInfo.templateVersion(), jobInfo.templateType());
        stmt.executeUpdate(query);
      }
      // Parameters table:
      // test_id, param name, param value
      for(String parameter: jobInfo.parameters().keySet()) {
        query = String.format("INSERT INTO COMPANY (TEST_ID, NAME, VALUE) "
            + "VALUES (%s, '%s', '%s');", testId, parameter, jobInfo.parameters().get(parameter));
        stmt.executeUpdate(query);
      }
      // Metrics table:
      // test_id, metric name, metric value
      for(String metric: metrics.keySet()) {
        query = String.format("INSERT INTO COMPANY (TEST_ID, NAME, VALUE) "
            + "VALUES (%s, '%s', %f);", testId, metric, metrics.get(metric));
        stmt.executeUpdate(query);
      }

      stmt.close();
      c.commit();
      c.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("Records added to postgres database successfully.");
  }

  /**
   * Checks if the input pcollection has the expected number of messages. 
   * 
   * @param jobId JobId of the job
   * @param metricName metric name of the input pcollection element count
   * @param expectedElements expected number of messages
   * @return whether the input pcollection has the expected number of messages.
   */
  public boolean shouldDrainJob(String jobId, String metricName, Long expectedElements) {
    try {
      Map<String, Double> metrics = dataflowClient.getMetrics(PROJECT, REGION, jobId);
      if (metrics.get(metricName) == (double)expectedElements) {
        return true;
      }
      LOG.info("Expected {} messages in input PCollection, but found {}.", expectedElements, metrics.get(metricName));
      return false;
    } catch (Exception e) {
      LOG.warn("Encountered error when trying to measure input elements. ", e);
      return false;
    }
  }

  private Map<String, Double> getMetrics(JobInfo jobInfo, String outputPtransform)
      throws IOException, ParseException, InterruptedException {
    // Metrics take upto 3 minutes to show up
    Thread.sleep(Duration.ofMinutes(3).toMillis());
    Map<String, Double> metrics = dataflowClient.getMetrics(PROJECT, REGION, jobInfo.jobId());
    LOG.info("Calculating approximate cost for {} under {}", jobInfo.jobId(), PROJECT);
    double cost = 0;
    if (jobInfo.jobType().equals("JOB_TYPE_STREAMING")) {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_STREAMING;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_STREAMING;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_STREAMING;
    } else {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_BATCH;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_BATCH;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_BATCH;
    }
    cost += metrics.get("TotalPdUsage") / 3600 * PD_PER_GB_HR;
    cost += metrics.get("TotalSsdUsage") / 3600 * PD_SSD_PER_GB_HR;
    metrics.put("cost", cost);
    metrics.put("avg_cpu_utilization", monitoringClient.getAvgCpuUtilization(PROJECT, jobInfo));
    metrics.put(
        "max_output_throughput",
        monitoringClient.getMaxThroughputOfPTransform(PROJECT, jobInfo, outputPtransform));
    LOG.info(formatForLogging(metrics));
    return metrics;
  }

  public static DataflowOperator.Config createConfig(JobInfo info, Duration timeout) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .setTimeoutAfter(timeout)
        .build();
  }
}
