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
package com.google.cloud.teleport.it.gcp;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.gcp.dataflow.ClassicTemplateClient;
import com.google.cloud.teleport.it.gcp.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import java.util.Collections;
import java.util.List;

/** Base class for Templates Load Tests. */
public class TemplateLoadTestBase extends LoadTestBase {

  public static final List<String> MACHINE_TYPES =
      List.of("n1-standard-1", "n1-standard-2", "n1-standard-4", "e2-standard-2", "e2-standard-4");

  PipelineLauncher launcher() {
    // If there is a TemplateLoadTest annotation, return appropriate dataflow template client
    TemplateLoadTest annotation = getClass().getAnnotation(TemplateLoadTest.class);
    if (annotation == null) {
      throw new RuntimeException(
          String.format(
              "%s did not specify which template is tested using @TemplateLoadTest.", getClass()));
    }
    Class<?> templateClass = annotation.value();
    Template[] templateAnnotations = templateClass.getAnnotationsByType(Template.class);
    if (templateAnnotations.length == 0) {
      throw new RuntimeException(
          String.format(
              "Template mentioned in @TemplateLoadTest for %s does not contain a @Template"
                  + " annotation.",
              getClass()));
    } else if (templateAnnotations[0].flexContainerName() != null
        && !templateAnnotations[0].flexContainerName().isEmpty()) {
      return FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();
    } else {
      return ClassicTemplateClient.builder().setCredentials(CREDENTIALS).build();
    }
  }

  protected LaunchConfig.Builder enableRunnerV2(LaunchConfig.Builder config) {
    return config.addEnvironment(
        "additionalExperiments", Collections.singletonList("use_runner_v2"));
  }

  protected LaunchConfig.Builder disableRunnerV2(LaunchConfig.Builder config) {
    return config.addEnvironment(
        "additionalExperiments", Collections.singletonList("disable_runner_v2"));
  }

  protected LaunchConfig.Builder enableStreamingEngine(LaunchConfig.Builder config) {
    return config.addEnvironment("enableStreamingEngine", true);
  }

  /** Options for Backlog load tests. */
  @AutoValue
  public abstract static class BacklogConfiguration {

    public abstract Integer getRowSize();

    public abstract Long getNumRows();

    public abstract Integer getGeneratorTimeout();

    public abstract Integer getPipelineTimeout();

    public static BacklogConfiguration of(long numRows, int generatorTimeout, int pipelineTimeout) {
      return new AutoValue_TemplateLoadTestBase_BacklogConfiguration.Builder()
          .setRowSize(1024)
          .setNumRows(numRows)
          .setGeneratorTimeout(generatorTimeout)
          .setPipelineTimeout(pipelineTimeout)
          .build();
    }

    public static BacklogConfiguration of(
        int rowSize, long numRows, int generatorTimeout, int pipelineTimeout) {
      return new AutoValue_TemplateLoadTestBase_BacklogConfiguration.Builder()
          .setRowSize(rowSize)
          .setNumRows(numRows)
          .setGeneratorTimeout(generatorTimeout)
          .setPipelineTimeout(pipelineTimeout)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setRowSize(int rowSize);

      abstract Builder setNumRows(long numRows);

      abstract Builder setGeneratorTimeout(int timeOutMinutes);

      abstract Builder setPipelineTimeout(int timeOutMinutes);

      abstract BacklogConfiguration build();
    }
  }
}
