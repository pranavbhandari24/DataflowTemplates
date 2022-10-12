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
package com.google.cloud.teleport.it.dataflow;

import com.google.api.services.dataflow.model.Job;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Client for working with Cloud Dataflow. */
public interface DataflowClient {
  /** Enum representing known Dataflow job states. */
  enum JobState {
    UNKNOWN("JOB_STATE_UNKNOWN"),
    STOPPED("JOB_STATE_STOPPED"),
    RUNNING("JOB_STATE_RUNNING"),
    DONE("JOB_STATE_DONE"),
    FAILED("JOB_STATE_FAILED"),
    CANCELLED("JOB_STATE_CANCELLED"),
    UPDATED("JOB_STATE_UPDATED"),
    DRAINING("JOB_STATE_DRAINING"),
    DRAINED("JOB_STATE_DRAINED"),
    PENDING("JOB_STATE_PENDING"),
    CANCELLING("JOB_STATE_CANCELLING"),
    QUEUED("JOB_STATE_QUEUED"),
    RESOURCE_CLEANING_UP("JOB_STATE_RESOURCE_CLEANING_UP");

    private static final String DATAFLOW_PREFIX = "JOB_STATE_";


    /** States that indicate the job is getting ready to run. */
    public static final ImmutableSet<JobState> PENDING_STATES =
        ImmutableSet.of(PENDING, QUEUED);

    /** States that indicate the job is running or getting ready to run. */
    public static final ImmutableSet<JobState> ACTIVE_STATES =
        ImmutableSet.of(RUNNING, UPDATED);

    /** States that indicate that the job is done. */
    public static final ImmutableSet<JobState> DONE_STATES =
        ImmutableSet.of(CANCELLED, DONE, DRAINED, FAILED, STOPPED);

    /** States that indicate that the job is in the process of finishing. */
    public static final ImmutableSet<JobState> FINISHING_STATES =
        ImmutableSet.of(DRAINING, CANCELLING);

    private final String text;

    JobState(String text) {
      this.text = text;
    }

    /**
     * Parses the state from Dataflow.
     *
     * <p>Always use this in place of valueOf.
     */
    public static JobState parse(String fromDataflow) {
      return valueOf(fromDataflow.replace(DATAFLOW_PREFIX, ""));
    }

    @Override
    public String toString() {
      return text;
    }
  }

  /** Config for starting a Dataflow template job. */
  class LaunchConfig {
    private final String jobName;
    private final ImmutableMap<String, String> parameters;
    private final String specPath;
    private final String sdk;
    private final String executable;
    private final String classname;

    private LaunchConfig(Builder builder) {
      this.jobName = builder.getJobName();
      this.parameters = ImmutableMap.copyOf(builder.getParameters());
      this.specPath = builder.getSpecPath();
      this.sdk = builder.getSdk();
      this.executable = builder.getExecutable();
      this.classname = builder.getClassname();
    }

    public String jobName() {
      return jobName;
    }

    public ImmutableMap<String, String> parameters() {
      return parameters;
    }

    @Nullable
    public String getParameter(String key) {
      return parameters.get(key);
    }

    public String specPath() {
      return specPath;
    }

    public String sdk() {
      return sdk;
    }

    public String executable() {
      return executable;
    }

    public String classname() {
      return classname;
    }

    public static Builder builder(String jobName) {
      return new Builder(jobName);
    }

    /** Builder for the {@link LaunchConfig}. */
    public static final class Builder {
      private final String jobName;
      private Map<String, String> parameters;
      private String specPath;
      private String sdk;
      private String executable;
      private String classname;

      private Builder(String jobName) {
        this.jobName = jobName;
        this.parameters = new HashMap<>();
      }

      public String getJobName() {
        return jobName;
      }

      public Map<String, String> getParameters() {
        return parameters;
      }

      public Builder setParameters(Map<String, String> value) {
        this.parameters = value;
        return this;
      }

      public Builder addParameter(String key, String value) {
        parameters.put(key, value);
        return this;
      }

      @Nullable
      public String getSpecPath() {
        return specPath;
      }

      public Builder setSpecPath(String specPath) {
        this.specPath = specPath;
        return this;
      }

      @Nullable
      public String getSdk() {
        return sdk;
      }

      public Builder setSdk(String sdk) {
        this.sdk = sdk;
        return this;
      }

      @Nullable
      public String getExecutable() {
        return executable;
      }

      public Builder setExecutable(String executable) {
        this.executable = executable;
        return this;
      }

      @Nullable
      public String getClassname() {
        return classname;
      }

      public Builder setClassname(String classname) {
        this.classname = classname;
        return this;
      }

      public LaunchConfig build() {
        return new LaunchConfig(this);
      }
    }
  }

  /** Info about the job from what Dataflow returned. */
  @AutoValue
  abstract class JobInfo {
    public abstract String jobId();

    public abstract JobState state();

    public abstract String startTime();

    public abstract String sdk();

    public abstract String version();

    public abstract String jobType();

    public abstract String runner();

    @Nullable  public abstract String templateName();

    @Nullable  public abstract String templateType();

    @Nullable public abstract String templateVersion();

    public abstract ImmutableMap<String, String> parameters();

    public static Builder builder() {
      return new AutoValue_DataflowClient_JobInfo.Builder();
    }

    /** Builder for {@link JobInfo}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setJobId(String value);

      public abstract Builder setState(JobState value);

      public abstract Builder setStartTime(String value);

      public abstract Builder setSdk(String value);

      public abstract Builder setVersion(String value);

      public abstract Builder setJobType(String value);

      public abstract Builder setRunner(String value);

      public abstract Builder setTemplateName(String value);

      public abstract Builder setTemplateType(String value);

      public abstract Builder setTemplateVersion(String value);

      public abstract Builder setParameters(ImmutableMap<String, String> value);

      public abstract JobInfo build();
    }
  }

  /**
   * Launches a new dataflow template job.
   *
   * @param project the project to run the job in
   * @param region the region to run the job in (e.g. us-east1)
   * @param options options for configuring the job
   * @return info about the request to launch a new job
   * @throws IOException if there is an issue sending the request
   */
  JobInfo launchTemplate(String project, String region, LaunchConfig options) throws IOException;

  /**
   * Launches a new dataflow job.
   *
   * @param project the project to run the job in
   * @param region the region to run the job in (e.g. us-east1)
   * @param options options for configuring the job
   * @return info about the request to launch a new job
   * @throws IOException if there is an issue sending the request
   */
  JobInfo launchJob(String project, String region, LaunchConfig options)
      throws IOException, InterruptedException;

  /**
   * Gets information of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @return job information
   * @throws IOException if there is an issue sending the request
   */
  Job getJob(String project, String region, String jobId) throws IOException;

  /**
   * Gets the current status of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @return the current state of the job
   * @throws IOException if there is an issue sending the request
   */
  JobState getJobStatus(String project, String region, String jobId) throws IOException;

  /**
   * Cancels the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to cancel
   * @throws IOException if there is an issue sending the request
   */
  void cancelJob(String project, String region, String jobId) throws IOException;

  /**
   * Drains the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job to cancel
   * @throws IOException if there is an issue sending the request
   */
  void drainJob(String project, String region, String jobId) throws IOException;

  /**
   * Get all metrics of the given job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId jobId of the job
   * @return all metrics of the given job
   * @throws IOException if there is an issue sending the request
   */
  Map<String, Double> getMetrics(String project, String region, String jobId)
      throws IOException;
}
