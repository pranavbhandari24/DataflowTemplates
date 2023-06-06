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
package com.google.cloud.teleport.it.gcp.datagenerator;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.auth.Credentials;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineLauncher.Sdk;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.gcp.dataflow.DefaultPipelineLauncher;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator.AutoscalingAlgorithmType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;

public class BacklogGenerator {

  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private final String jobName;
  private final Map<String, String> parameters;
  private final PipelineLauncher pipelineLauncher;
  private final PipelineOperator pipelineOperator;

  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"backlogGenerator\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvroLine\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"row\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private BacklogGenerator(Builder builder) {
    pipelineLauncher = DefaultPipelineLauncher.builder().setCredentials(CREDENTIALS).build();
    pipelineOperator = new PipelineOperator(pipelineLauncher);
    this.jobName = builder.jobName;
    this.parameters = builder.parameters;
  }

  public static BacklogGenerator.Builder builder(String testName) {
    return new BacklogGenerator.Builder(testName)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
  }

  public void execute(Duration timeout) throws IOException {
    checkArgument(parameters.get("sinkType") != null, "Missing required value --sinkType");
    Pipeline pipeline = Pipeline.create();
    SyntheticSourceOptions syntheticSourceOptions =
        SyntheticSourceOptions.fromJsonString(getSourceOptions(), SyntheticSourceOptions.class);
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    PCollection<byte[]> rows =
        pipeline
            .apply(
                "Read from source", Read.from(new SyntheticBoundedSource(syntheticSourceOptions)))
            .apply("Map records", ParDo.of(new MapKVToV()));
    switch (parameters.get("sinkType").toUpperCase()) {
      case "PUBSUB":
        checkArgument(
            parameters.get("topic") != null, "Missing required value --topic for PUBSUB sink type");
        rows.apply("Convert to Json String", ParDo.of(new ConvertToJsonString()))
            .apply("Write to Pubsub", PubsubIO.writeStrings().to(parameters.get("topic")));
        break;
      case "SPANNER":
        checkArgument(
            parameters.get("spannerInstanceName") != null,
            "Missing required value --spannerInstanceName for SPANNER sink type");
        checkArgument(
            parameters.get("spannerDatabaseName") != null,
            "Missing required value --spannerDatabaseName for SPANNER sink type");
        checkArgument(
            parameters.get("spannerTableName") != null,
            "Missing required value --spannerTableName for SPANNER sink type");
        rows.apply(
                "Convert to Mutations",
                ParDo.of(new ConvertToMutation(parameters.get("spannerTableName"))))
            .apply(
                "Write to Spanner",
                SpannerIO.write()
                    .withInstanceId(parameters.get("spannerInstanceName"))
                    .withDatabaseId(parameters.get("spannerDatabaseName")));
        break;
      case "GCS":
        checkArgument(
            parameters.get("gcsWriteFormat") != null,
            "Missing required value --gcsWriteFormat for GCS sink type");
        checkArgument(
            parameters.get("outputDirectory") != null,
            "Missing required value --outputDirectory for GCS sink type");
        switch (parameters.get("gcsWriteFormat")) {
          case "JSON":
            rows.apply("Convert to Json String", ParDo.of(new ConvertToJsonString()))
                .apply(
                    "Write json files to GCS",
                    TextIO.write()
                        .withWindowedWrites()
                        .to(parameters.get("outputDirectory") + "/")
                        .withNumShards(
                            Integer.parseInt(
                                parameters.getOrDefault(
                                    "numShards",
                                    "0")))); // default value 0 for numShards (allows the system to
            // decide the number of shards)
            break;
          case "AVRO":
            rows.apply("Convert to GenericRecord", ParDo.of(new ConvertToGenericRecord()))
                .setCoder(AvroCoder.of(SCHEMA))
                .apply(
                    "Write avro files to GCS",
                    AvroIO.writeGenericRecords(SCHEMA)
                        .to(parameters.get("outputDirectory") + "/")
                        .withSuffix(".avro"));
            break;
          case "PARQUET":
            rows.apply("Convert to GenericRecord", ParDo.of(new ConvertToGenericRecord()))
                .setCoder(AvroCoder.of(SCHEMA))
                .apply(
                    "Write avro files to GCS",
                    FileIO.<GenericRecord>write()
                        .via(ParquetIO.sink(SCHEMA))
                        .to(parameters.get("outputDirectory")));
            break;
        }
        break;
      case "BIGQUERY":
        checkArgument(
            parameters.get("bigQueryWriteFormat") != null,
            "Missing required value --bigQueryWriteFormat for BIGQUERY sink type");
        checkArgument(
            parameters.get("bigQueryOutputTableQualifier") != null,
            "Missing required value --bigQueryOutputTableQualifier for BIGQUERY sink type");
        checkArgument(
            parameters.get("bigQueryWriteMethod") != null,
            "Missing required value --bigQueryWriteMethod for BIGQUERY sink type");
        checkArgument(
            parameters.get("bigQueryTempLocation") != null,
            "Missing required value --bigQueryTempLocation for BIGQUERY sink type");
        if (parameters
            .get("bigQueryWriteMethod")
            .equals(Write.Method.STREAMING_INSERTS.toString())) {
          pipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
        }
        BigQueryIO.Write<byte[]> writeIO = null;
        switch (parameters.get("bigQueryWriteFormat")) {
          case "AVRO":
            writeIO =
                BigQueryIO.<byte[]>write()
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withAvroFormatFunction(
                        writeRequest -> {
                          byte[] data = writeRequest.getElement();
                          GenericRecord record = new GenericData.Record(writeRequest.getSchema());
                          record.put("data", ByteBuffer.wrap(data));
                          return record;
                        });
            break;
          case "JSON":
            writeIO =
                BigQueryIO.<byte[]>write()
                    .withSuccessfulInsertsPropagation(false)
                    .withFormatFunction(
                        input -> {
                          TableRow tableRow = new TableRow();
                          tableRow.set("data", Base64.getEncoder().encodeToString(input));
                          return tableRow;
                        });
            break;
        }
        rows.apply(
            "Write to BQ",
            writeIO
                .to(parameters.get("bigQueryOutputTableQualifier"))
                .withMethod(Write.Method.valueOf(parameters.get("bigQueryWriteMethod")))
                .withSchema(
                    new TableSchema()
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("data").setType("BYTES"))))
                .withCustomGcsTempLocation(
                    ValueProvider.StaticValueProvider.of(parameters.get("bigQueryTempLocation"))));
        break;
      case "JDBC":
        checkArgument(
            parameters.get("driverClassName") != null,
            "Missing required value --driverClassName for JDBC sink type");
        checkArgument(
            parameters.get("connectionUrl") != null,
            "Missing required value --connectionUrl for JDBC sink type");
        checkArgument(
            parameters.get("statement") != null,
            "Missing required value --statement for JDBC sink type");
        JdbcIO.DataSourceConfiguration dataSourceConfiguration =
            JdbcIO.DataSourceConfiguration.create(
                parameters.get("driverClassName"), parameters.get("connectionUrl"));
        if (parameters.containsKey("username")) {
          dataSourceConfiguration =
              dataSourceConfiguration.withUsername(parameters.get("username"));
        }
        if (parameters.containsKey("password")) {
          dataSourceConfiguration =
              dataSourceConfiguration.withPassword(parameters.get("password"));
        }
        if (parameters.containsKey("connectionProperties")) {
          dataSourceConfiguration =
              dataSourceConfiguration.withConnectionProperties(
                  parameters.get("connectionProperties"));
        }

        rows.apply(
            "Write To Jdbc",
            JdbcIO.<byte[]>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withStatement(parameters.get("statement"))
                .withPreparedStatementSetter(
                    (PreparedStatementSetter<byte[]>)
                        (element, query) -> {
                          query.setBinaryStream(
                              1, new ByteArrayInputStream(element), element.length);
                        }));
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("%s sinkType is not supported.", parameters.get("sinkType")));
    }

    LaunchConfig options =
        LaunchConfig.builder(jobName + "-data-generator")
            .setSdk(Sdk.JAVA)
            .setPipeline(pipeline)
            .addParameter("runner", "DataflowRunner")
            .addParameter("maxNumWorkers", "100")
            .build();

    LaunchInfo launchInfo =
        pipelineLauncher.launch(TestProperties.project(), TestProperties.region(), options);
    assertThatPipeline(launchInfo).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(launchInfo, timeout));
    assertThatResult(result).isLaunchFinished();
  }

  private String getSourceOptions() {
    return String.format(
        "{\"numRecords\":%s,\"keySizeBytes\":1,\"valueSizeBytes\":%s}",
        parameters.get("numRows"), parameters.get("rowSize"));
  }

  public static PipelineOperator.Config createConfig(LaunchInfo info, Duration timeout) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(TestProperties.project())
        .setRegion(TestProperties.region())
        .setTimeoutAfter(timeout)
        .build();
  }

  private static class MapKVToV extends DoFn<KV<byte[], byte[]>, byte[]> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(context.element().getValue());
    }
  }

  private static class ConvertToJsonString extends DoFn<byte[], String> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(new JSONObject().put("bytes", new String(context.element())).toString());
    }
  }

  private static class ConvertToGenericRecord extends DoFn<byte[], GenericRecord> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(
          new GenericRecordBuilder(SCHEMA).set("row", new String(context.element())).build());
    }
  }

  private static class ConvertToMutation extends DoFn<byte[], Mutation> {
    String tableName;

    ConvertToMutation(String tableName) {
      this.tableName = tableName;
    }

    @ProcessElement
    public void process(ProcessContext context) {
      context.output(
          Mutation.newInsertOrUpdateBuilder(tableName)
              .set("bytes")
              .to(new String(context.element()))
              .build());
    }
  }

  /** Builder for the {@link BacklogGenerator}. */
  public static final class Builder {
    private final String jobName;
    private final Map<String, String> parameters;

    public Builder(String jobName) {
      this.jobName = jobName;
      this.parameters = new HashMap<>();
    }

    // common options
    public Builder setNumRows(long numRows) {
      parameters.put("numRows", String.valueOf(numRows));
      return this;
    }

    public Builder setRowSize(int rowSize) {
      parameters.put("rowSize", String.valueOf(rowSize));
      return this;
    }

    public Builder setSinkType(String value) {
      parameters.put("sinkType", value);
      return this;
    }

    // Pub/Sub options
    public Builder setTopic(String value) {
      parameters.put("topic", value);
      return this;
    }

    // Spanner options
    public Builder setSpannerInstanceName(String value) {
      parameters.put("spannerInstanceName", value);
      return this;
    }

    public Builder setSpannerDatabaseName(String value) {
      parameters.put("spannerDatabaseName", value);
      return this;
    }

    public Builder setSpannerTableName(String value) {
      parameters.put("spannerTableName", value);
      return this;
    }

    // GCS options
    public Builder setGcsWriteFormat(GcsWriteFormat value) {
      parameters.put("gcsWriteFormat", value.toString());
      return this;
    }

    public Builder setOutputDirectory(String value) {
      parameters.put("outputDirectory", value);
      return this;
    }

    public Builder setNumShards(String value) {
      parameters.put("numShards", value);
      return this;
    }

    // BigQuery options
    public Builder setBigQueryWriteFormat(BigQueryWriteFormat value) {
      parameters.put("bigQueryWriteFormat", value.toString());
      return this;
    }

    public Builder setBigQueryOutputTableQualifier(String value) {
      parameters.put("bigQueryOutputTableQualifier", value);
      return this;
    }

    public Builder setBigQueryWriteMethod(BigQueryIO.Write.Method value) {
      parameters.put("bigQueryMethod", value.toString());
      return this;
    }

    public Builder setBigQueryTempLocation(String value) {
      parameters.put("bigQueryTempLocation", value);
      return this;
    }

    // Jdbc options
    public Builder setDriverClassName(String value) {
      parameters.put("driverClassName", value);
      return this;
    }

    public Builder setConnectionUrl(String value) {
      parameters.put("connectionUrl", value);
      return this;
    }

    public Builder setStatement(String value) {
      parameters.put("statement", value);
      return this;
    }

    public Builder setUsername(String value) {
      parameters.put("username", value);
      return this;
    }

    public Builder setPassword(String value) {
      parameters.put("password", value);
      return this;
    }

    public Builder setConnectionProperites(String value) {
      parameters.put("connectionProperties", value);
      return this;
    }

    public Builder setAutoscalingAlgorithm(AutoscalingAlgorithmType autoscalingAlgorithm) {
      parameters.put("autoscalingAlgorithm", autoscalingAlgorithm.toString());
      return this;
    }

    public BacklogGenerator build() {
      return new BacklogGenerator(this);
    }
  }

  public enum GcsWriteFormat {
    JSON,
    AVRO,
    PARQUET
  }

  public enum BigQueryWriteFormat {
    AVRO,
    JSON
  }
}
