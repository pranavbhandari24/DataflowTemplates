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
package com.google.cloud.teleport.it.dataproc;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.AutoscalingPolicy;
import com.google.cloud.dataproc.v1.AutoscalingPolicyServiceClient;
import com.google.cloud.dataproc.v1.AutoscalingPolicyServiceSettings;
import com.google.cloud.dataproc.v1.Cluster;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.dataproc.v1.RegionName;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for working with Google Cloud Dataproc. */
public final class DataprocClient {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);
  private final ClusterControllerClient clusterControllerClient;
  private final AutoscalingPolicyServiceClient autoscalingPolicyServiceClient;
  private final JobControllerClient jobControllerClient;

  private DataprocClient(Builder builder) throws IOException {
    String endpoint = String.format("%s-dataproc.googleapis.com:443", builder.getRegion());
    ClusterControllerSettings clusterControllerSettings =
        ClusterControllerSettings.newBuilder()
            .setEndpoint(endpoint)
            .setCredentialsProvider(builder.getCredentialsProvider())
            .build();
    AutoscalingPolicyServiceSettings autoscalingPolicyServiceSettings =
        AutoscalingPolicyServiceSettings.newBuilder()
            .setEndpoint(endpoint)
            .setCredentialsProvider(builder.getCredentialsProvider())
            .build();
    JobControllerSettings jobControllerSettings =
        JobControllerSettings.newBuilder()
            .setEndpoint(endpoint)
            .setCredentialsProvider(builder.getCredentialsProvider())
            .build();
    this.clusterControllerClient = ClusterControllerClient.create(clusterControllerSettings);
    this.autoscalingPolicyServiceClient =
        AutoscalingPolicyServiceClient.create(autoscalingPolicyServiceSettings);
    this.jobControllerClient = JobControllerClient.create(jobControllerSettings);
  }

  private DataprocClient(
      ClusterControllerClient clusterControllerClient,
      AutoscalingPolicyServiceClient autoscalingPolicyServiceClient,
      JobControllerClient jobControllerClient) {
    this.clusterControllerClient = clusterControllerClient;
    this.autoscalingPolicyServiceClient = autoscalingPolicyServiceClient;
    this.jobControllerClient = jobControllerClient;
  }

  public static DataprocClient withClients(
      ClusterControllerClient clusterControllerClient,
      AutoscalingPolicyServiceClient autoscalingPolicyServiceClient,
      JobControllerClient jobControllerClient) {
    return new DataprocClient(
        clusterControllerClient, autoscalingPolicyServiceClient, jobControllerClient);
  }

  public static Builder builder() {
    return new Builder();
  }

  public Cluster createCluster(
      String project, String region, String clusterName, ClusterConfig clusterConfig)
      throws ExecutionException, InterruptedException {
    LOG.info("Creating Dataproc cluster with provided config under {}", project);
    Cluster cluster =
        Cluster.newBuilder().setClusterName(clusterName).setConfig(clusterConfig).build();
    OperationFuture<Cluster, ClusterOperationMetadata> createClusterAsyncRequest =
        clusterControllerClient.createClusterAsync(project, region, cluster);
    return createClusterAsyncRequest.get();
  }

  public void createAutoscalingPolicy(
      String project, String region, AutoscalingPolicy autoscalingPolicy) {
    RegionName parent = RegionName.of(project, region);
    autoscalingPolicyServiceClient.createAutoscalingPolicy(parent, autoscalingPolicy);
  }

  public Job submitJob(String project, String region, Job job)
      throws ExecutionException, InterruptedException {
    OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
        jobControllerClient.submitJobAsOperationAsync(project, region, job);
    return submitJobAsOperationAsyncRequest.get();
  }

  public synchronized void cleanupAll() {
    LOG.info("Attempting to cleanup dataproc clients.");
    clusterControllerClient.close();
    autoscalingPolicyServiceClient.close();
    jobControllerClient.close();
    LOG.info("Dataproc clients successfully cleaned up.");
  }

  /** Builder for {@link DataprocClient}. */
  public static final class Builder {
    private String region;

    private CredentialsProvider credentialsProvider;

    private Builder() {}

    public String getRegion() {
      return region;
    }

    public Builder setRegion(String value) {
      region = value;
      return this;
    }

    public CredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    public Builder setCredentialsProvider(CredentialsProvider value) {
      credentialsProvider = value;
      return this;
    }

    public DataprocClient build() throws IOException {
      return new DataprocClient(this);
    }
  }
}
