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
package com.google.cloud.teleport.it.monitoring;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for working with Google Cloud Monitoring. */
public final class MonitoringClient {
  private static final Logger LOG = LoggerFactory.getLogger(MonitoringClient.class);
  private final MetricServiceClient metricServiceClient;

  private MonitoringClient(Builder builder) throws IOException {
    MetricServiceSettings metricServiceSettings =
        MetricServiceSettings.newBuilder()
            .setCredentialsProvider(builder.getCredentialsProvider())
            .build();
    this.metricServiceClient = MetricServiceClient.create(metricServiceSettings);
  }

  private MonitoringClient(MetricServiceClient metricServiceClient) {
    this.metricServiceClient = metricServiceClient;
  }

  public static MonitoringClient withMonitoringClient(MetricServiceClient metricServiceClient) {
    return new MonitoringClient(metricServiceClient);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Lists time series that match a filter.
   *
   * @param request time series request to execute
   * @return time series response
   * @throws IOException if there is an issue sending the request
   */
  public List<Double> listTimeSeries(ListTimeSeriesRequest request) throws IOException {
    return extractValuesFromTimeSeries(metricServiceClient.listTimeSeries(request));
  }

  /**
   * Calculates the average CPU Utilization for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobInfo information about the job
   * @return Average CPU Utilization for the given job.
   * @throws IOException if any error is encountered
   * @throws ParseException if timestamp is inaccurate
   */
  public Double getAvgCpuUtilization(String project, JobInfo jobInfo)
      throws IOException, ParseException {
    LOG.info("Getting avg CPU utilization for {} under {}", jobInfo.jobId(), project);
    String filter =
        String.format(
            "metric.type = \"compute.googleapis.com/instance/cpu/utilization\" "
                + "AND resource.labels.project_id = \"%s\" "
                + "AND metadata.user_labels.dataflow_job_id = \"%s\"",
            project, jobInfo.jobId());
    TimeInterval timeInterval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.parse(jobInfo.startTime()))
            .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aggregation.Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_MEAN)
            .addGroupByFields("resource.instance_id")
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeries(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to calculate avg CPU utilization.");
      return null;
    }
    // multiplying by 100 to get percentage
    return calculateAverage(timeSeries) * 100;
  }

  /**
   * Get max throughput from a particular PTransform during job run interval.
   *
   * @param project the project that the job is running under
   * @param pTransformName name of the Ptransform
   * @param jobInfo information about the job
   * @return Max throughput of PTransform across time interval
   */
  public Double getMaxThroughputOfPTransform(String project, JobInfo jobInfo, String pTransformName)
      throws IOException, ParseException, InterruptedException {
    LOG.info("Getting max throughput for {} under {}", jobInfo.jobId(), project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/elements_produced_count\" "
                + "AND metric.labels.job_id=\"%s\" "
                + "AND metric.labels.ptransform=\"%s\" ",
            jobInfo.jobId(), pTransformName);
    TimeInterval timeInterval =
        TimeInterval.newBuilder()
            .setStartTime(Timestamps.parse(jobInfo.startTime()))
            .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
            .build();
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(Duration.newBuilder().setSeconds(60).build())
            .setPerSeriesAligner(Aggregation.Aligner.ALIGN_RATE)
            .build();
    ListTimeSeriesRequest request =
        ListTimeSeriesRequest.newBuilder()
            .setName(ProjectName.of(project).toString())
            .setFilter(filter)
            .setInterval(timeInterval)
            .setAggregation(aggregation)
            .build();
    List<Double> timeSeries = listTimeSeries(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to calculate max throughput.");
      return null;
    }
    return Collections.max(timeSeries);
  }

  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup monitoring client.");
    metricServiceClient.close();
    LOG.info("Monitoring client successfully cleaned up.");
  }


  private List<Double> extractValuesFromTimeSeries(ListTimeSeriesPagedResponse response) {
    List<Double> values = new ArrayList<>();
    for (TimeSeries ts : response.iterateAll()) {
      for (Point point : ts.getPointsList()) {
        values.add(point.getValue().getDoubleValue());
      }
    }
    return values;
  }

  private Double calculateAverage(List<Double> values) {
    return values.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  /** Builder for {@link MonitoringClient}. */
  public static final class Builder {
    private CredentialsProvider credentialsProvider;

    private Builder() {}

    public CredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    public Builder setCredentialsProvider(CredentialsProvider value) {
      credentialsProvider = value;
      return this;
    }

    public MonitoringClient build() throws IOException {
      return new MonitoringClient(this);
    }
  }
}
