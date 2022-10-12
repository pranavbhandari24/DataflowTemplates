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

import static com.google.cloud.teleport.it.dataflow.DataflowClient.JobState.FAILED;
import static com.google.cloud.teleport.it.dataflow.DataflowClient.JobState.PENDING_STATES;
import static com.google.cloud.teleport.it.logging.LogStrings.formatForLogging;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.logging.LogStrings;
import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link DataflowClient} interface.
 */
public class DefaultDataflowClient implements DataflowClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataflowClient.class);
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("Submitted job: (\\S+)");
  protected final Dataflow client;

  DefaultDataflowClient(Dataflow client) {
    this.client = client;
  }

  private DefaultDataflowClient(DefaultDataflowClient.Builder builder) {
    this(
        new Dataflow(
            Utils.getDefaultTransport(),
            Utils.getDefaultJsonFactory(),
            new HttpCredentialsAdapter(builder.getCredentials())));
  }

  public static DefaultDataflowClient withDataflowClient(Dataflow client) {
    return new DefaultDataflowClient(client);
  }

  public static DefaultDataflowClient.Builder builder() {
    return new DefaultDataflowClient.Builder();
  }

  @Override
  public JobInfo launchTemplate(String project, String region, LaunchConfig options)
      throws IOException {
    checkState(options.specPath() != null,
        "Cannot launch a template job without specPath. Please specify specPath and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the spec at {}", options.specPath());
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));

    LaunchFlexTemplateParameter parameter =
        new LaunchFlexTemplateParameter()
            .setJobName(options.jobName())
            .setParameters(options.parameters())
            .setContainerSpecGcsPath(options.specPath());
    LaunchFlexTemplateRequest request =
        new LaunchFlexTemplateRequest().setLaunchParameter(parameter);
    LOG.info("Sending request:\n{}", formatForLogging(request));

    LaunchFlexTemplateResponse response =
        client.projects().locations().flexTemplates().launch(project, region, request).execute();
    LOG.info("Received response:\n{}", formatForLogging(response));

    Job job = response.getJob();
    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, job.getId());
    job = getJob(project, region, job.getId());
    return getJobInfo(options, state, job, /*runner*/ "Dataflow");
  }

  @Override
  public JobInfo launchJob(String project, String region, LaunchConfig options)
      throws IOException, InterruptedException {
    checkState(options.sdk() != null, "Cannot launch a dataflow job "
        + "without sdk specified. Please specify sdk and try again!");
    checkState(options.executable() !=null, "Cannot launch a dataflow job "
        + "without executable specified. Please specify executable and try again!");
    LOG.info("Getting ready to launch {} in {} under {}", options.jobName(), region, project);
    LOG.info("Using the executable at {}", options.executable());
    LOG.info("Using parameters:\n{}", formatForLogging(options.parameters()));
    // Create SDK specific command and execute to launch dataflow job
    List<String> cmd = new ArrayList<>();
    switch (options.sdk().toLowerCase()) {
      case "java":
        checkState(options.classname() !=null, "Cannot launch a dataflow job "
            + "without classname specified. Please specify classname and try again!");
        cmd.add("java");
        cmd.add("-cp");
        cmd.add(options.executable());
        cmd.add(options.classname());
        break;
      case "python":
        cmd.add("python3");
        cmd.add(options.executable());
        break;
      case "go":
        cmd.add("go");
        cmd.add("run");
        cmd.add(options.executable());
        break;
      default:
        throw new RuntimeException(String.format("Invalid sdk %s specified. "
            + "sdk can be one of java, python, or go.", options.sdk()));
    }
    for(String parameter: options.parameters().keySet()) {
      cmd.add(String.format("--%s=%s", parameter, options.getParameter(parameter)));
    }
    cmd.add(String.format("--project=%s", project));
    cmd.add(String.format("--region=%s", region));
    String jobId = executeCommandAndParseResponse(cmd);
    // Wait until the job is active to get more information
    JobState state = waitUntilActive(project, region, jobId);
    Job job = getJob(project, region, jobId);
    return getJobInfo(options, state, job, options.getParameter("runner"));
  }

  @Override
  public Job getJob(String project, String region, String jobId) throws IOException {
    LOG.info("Getting the status of {} under {}", jobId, project);
    Job job = client.projects().locations().jobs().get(project, region, jobId).execute();
    LOG.info("Received job on get request for {}:\n{}", jobId, LogStrings.formatForLogging(job));
    return job;
  }

  @Override
  public JobState getJobStatus(String project, String region, String jobId) throws IOException {
    return handleJobState(getJob(project, region, jobId));
  }

  @Override
  public void cancelJob(String project, String region, String jobId) throws IOException {
    LOG.info("Cancelling {} under {}", jobId, project);
    Job job = new Job().setRequestedState(JobState.CANCELLED.toString());
    LOG.info("Sending job to update {}:\n{}", jobId, LogStrings.formatForLogging(job));
    client.projects().locations().jobs().update(project, region, jobId, job).execute();
  }

  @Override
  public void drainJob(String project, String region, String jobId) throws IOException {
    LOG.info("Draining {} under {}", jobId, project);
    Job job = new Job().setRequestedState(JobState.DRAINED.toString());
    LOG.info("Sending job to update {}:\n{}", jobId, LogStrings.formatForLogging(job));
    client.projects().locations().jobs().update(project, region, jobId, job).execute();
  }

  @Override
  public Map<String, Double> getMetrics(String project, String region, String jobId) throws IOException {
    LOG.info("Getting metrics for {} under {}", jobId, project);
    List<MetricUpdate> metrics =
        client
            .projects()
            .locations()
            .jobs()
            .getMetrics(project, region, jobId)
            .execute()
            .getMetrics();
    Map<String, Double> result = new HashMap<>();
    for (MetricUpdate metricUpdate : metrics) {
      String metricName = metricUpdate.getName().getName();
      // Since we query metrics after the job finishes, we can ignore tentative and step metrics
      if (metricUpdate.getName().getContext().containsKey("tentative")
          || metricUpdate.getName().getContext().containsKey("execution_step")
          || metricUpdate.getName().getContext().containsKey("step")
          || metricName.equals("MeanByteCount")) {
        continue;
      }

      // Rename ElementCount to use original name to prevent overwriting
      // We need this for detecting when to stop streaming jobs
      if (metricName.equals("ElementCount")) {
        metricName = metricUpdate.getName().getContext().get("original_name");
      }

      if (result.containsKey(metricName)) {
        LOG.warn("Key {} already exists in metrics. Something might be wrong.", metricName);
      }

      if (metricUpdate.getScalar() != null) {
        result.put(metricName, ((Number) metricUpdate.getScalar()).doubleValue());
      } else if (metricUpdate.getDistribution() != null) {
        // currently, reporting distribution metrics as 4 separate scalar metrics
        ArrayMap distributionMap = (ArrayMap) metricUpdate.getDistribution();
        result.put(metricName + "_COUNT", ((Number) distributionMap.get("count")).doubleValue());
        result.put(metricName + "_MIN", ((Number) distributionMap.get("min")).doubleValue());
        result.put(metricName + "_MAX", ((Number) distributionMap.get("max")).doubleValue());
        result.put(metricName + "_SUM", ((Number) distributionMap.get("sum")).doubleValue());
      } else if (metricUpdate.getGauge() != null) {
        LOG.warn("Gauge metric {} cannot be handled.", metricName);
        // not sure how to handle gauge metrics
      }
    }
    return result;
  }

  /** Creates a JobInfo object from the provided parameters. */
  private JobInfo getJobInfo(LaunchConfig options, JobState state, Job job, String runner) {
    return JobInfo.builder()
        .setJobId(job.getId())
        .setStartTime(job.getCreateTime())
        .setSdk(job.getJobMetadata().getSdkVersion().getVersionDisplayName())
        .setVersion(job.getJobMetadata().getSdkVersion().getVersion())
        .setJobType(job.getType())
        .setRunner(runner)
        .setParameters(options.parameters())
        .setState(state)
        .build();
  }

  /** Executes the specified command and parses the response to get the Job ID. */
  private String executeCommandAndParseResponse(List<String> cmd)
      throws IOException {
    Process process = new ProcessBuilder().command(cmd).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    Matcher m = JOB_ID_PATTERN.matcher(output);
    if (!m.find()) {
      throw new RuntimeException(
          String.format("Dataflow output in unexpected format. Failed to parse Dataflow Job ID. "
              + "Result from process: %s", output));
    }
    String jobId = m.group(1);
    LOG.info("Submitted job: {}", jobId);
    return jobId;
  }

  /** Parses the job state if available or returns {@link JobState#UNKNOWN} if not given. */
  private static JobState handleJobState(Job job) {
    String currentState = job.getCurrentState();
    return Strings.isNullOrEmpty(currentState) ? JobState.UNKNOWN : JobState.parse(currentState);
  }

  /** Waits until the specified job is not in a pending state. */
  private JobState waitUntilActive(String project, String region, String jobId) throws IOException {
    JobState state = getJobStatus(project, region, jobId);
    while(PENDING_STATES.contains(state)) {
      LOG.info("Job still pending. Will check again in 30 seconds");
      try {
        Thread.sleep(Duration.ofSeconds(30).toMillis());
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }
      state = getJobStatus(project, region, jobId);
    }
    if (state == FAILED) {
      throw new RuntimeException(String.format("The job failed before launch! For more "
          + "information please check if the job log for Job ID: %s, under project %s.", jobId,
          project));
    }
    return state;
  }

  /** Builder for {@link DefaultDataflowClient}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public DefaultDataflowClient.Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public DefaultDataflowClient build() {
      return new DefaultDataflowClient(this);
    }
  }
}
