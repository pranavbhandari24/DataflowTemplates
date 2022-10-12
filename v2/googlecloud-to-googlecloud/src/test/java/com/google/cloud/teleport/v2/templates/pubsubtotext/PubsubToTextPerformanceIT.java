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
package com.google.cloud.teleport.v2.templates.pubsubtotext;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.PerformanceBenchmarkingBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.common.base.MoreObjects;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for testing performance of {@link PubsubToText} (Cloud_PubSub_to_GCS_Text_Flex).
 */
@RunWith(JUnit4.class)
public final class PubsubToTextPerformanceIT extends PerformanceBenchmarkingBase {

  @Rule public final TestName testName = new TestName();
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String SPEC_PATH = MoreObjects.firstNonNull(
      "gs://dataflow-templates/latest/flex/Cloud_PubSub_to_GCS_Text_Flex",
      TestProperties.specPath());
  private static final String TEST_ROOT_DIR = PubsubToTextPerformanceIT.class.getSimpleName();
  private static final String INPUT_SUBSCRIPTION = "inputSubscription";
  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String NUM_WORKERS_KEY = "numWorkers";
  private static final String MAX_WORKERS_KEY = "maxWorkers";
  private static final String OUTPUT_FILENAME_PREFIX = "outputFilenamePrefix";
  private static final String DEFAULT_WINDOW_DURATION = "10s";
  private static final Pattern EXPECTED_PATTERN = Pattern.compile(".*subscription-output-.*");
  private static final String SCHEMA_LOCATION = "gs://cloud-teleport-testing/PerformanceTests/fakeDataSchema.json";
  private static final String INPUT_METRIC = "Read PubSub Events/MapElements/Map-out0-ElementCount";
  private static final String OUTPUT_PTRANSFORM = "Write File(s)/WriteFiles/WriteShardedBundlesToTempFiles/ApplyShardingKey";

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient artifactClient;

  @Before
  public void setup() throws IOException {
    Storage gcsClient = createGcsClient(CREDENTIALS);
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void tearDown() {
    artifactClient.cleanupRun();
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testTopicToGcsBacklogPerformance()
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    DataGenerator dataGenerator = DataGenerator.builder(jobName + "-data-generator")
        .setSchemaLocation(SCHEMA_LOCATION)
        .setQPS("1000000")
        .setMessagesLimit("30000000")
        .setTopic(topic.toString())
        .setNumWorkers("50")
        .setMaxNumWorkers("150")
        .setAutoscalingAlgorithm("THROUGHPUT_BASED")
        .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName)
            .setSpecPath(SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "20")
            .addParameter(MAX_WORKERS_KEY, "100")
            .build();

    // Act
    dataGenerator.execute(Duration.ofMinutes(30));
    JobInfo info = dataflowClient.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Result result =
        dataflowOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(30)),
            () -> {
              artifacts.set(artifactClient.listArtifacts(name, EXPECTED_PATTERN));
              // check if the pipeline has all messages and validate that there are files in GCS
              return shouldDrainJob(info.jobId(), INPUT_METRIC, 10000000L) &&
                  !artifacts.get().isEmpty();
            });
    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);

    // export results
    exportMetricsToBigQuery(info, name, OUTPUT_PTRANSFORM);
  }

  @Test
  public void testTopicToGcsBacklogPerformanceUsingStreamingEngine()
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    DataGenerator dataGenerator = DataGenerator.builder(jobName + "-data-generator")
        .setSchemaLocation(SCHEMA_LOCATION)
        .setQPS("1000000")
        .setMessagesLimit("30000000")
        .setTopic(topic.toString())
        .setNumWorkers("50")
        .setMaxNumWorkers("150")
        .setAutoscalingAlgorithm("THROUGHPUT_BASED")
        .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName)
            .setSpecPath(SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "20")
            .addParameter(MAX_WORKERS_KEY, "100")
            .addParameter("enableStreamingEngine", "true")
            .build();

    // Act
    dataGenerator.execute(Duration.ofMinutes(30));
    JobInfo info = dataflowClient.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Result result =
        dataflowOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(30)),
            () -> {
              artifacts.set(artifactClient.listArtifacts(name, EXPECTED_PATTERN));
              // check if the pipeline has all messages and validate that there are files in GCS
              return shouldDrainJob(info.jobId(), INPUT_METRIC, 10000000L) &&
                  !artifacts.get().isEmpty();
            });
    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);

    // export results
    exportMetricsToBigQuery(info, name, OUTPUT_PTRANSFORM);
  }

  @Test
  public void testTopicToGcsSteadyStatePerformance1hr()
      throws IOException, InterruptedException, ParseException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    DataGenerator dataGenerator = DataGenerator.builder(jobName + "-data-generator")
        .setSchemaLocation(SCHEMA_LOCATION)
        .setQPS("100000")
        .setTopic(topic.toString())
        .setNumWorkers("10")
        .setMaxNumWorkers("100")
        .setAutoscalingAlgorithm("THROUGHPUT_BASED")
        .build();
    LaunchConfig options =
        LaunchConfig.builder(jobName)
            .setSpecPath(SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .addParameter(NUM_WORKERS_KEY, "10")
            .addParameter(MAX_WORKERS_KEY, "100")
            .build();

    // Act
    JobInfo info = dataflowClient.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);
    dataGenerator.execute(Duration.ofMinutes(60));
    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    // Validate that the template executed as expected
    Result result =
        dataflowOperator.waitForConditionAndFinish(
            createConfig(info, Duration.ofMinutes(5)),
            () -> {
              artifacts.set(artifactClient.listArtifacts(name, EXPECTED_PATTERN));
              return !artifacts.get().isEmpty();
            });
    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
    // export results
    exportMetricsToBigQuery(info, name, OUTPUT_PTRANSFORM);
  }

  private static String getTestMethodDirPath(String testMethod) {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testMethod);
  }
}
