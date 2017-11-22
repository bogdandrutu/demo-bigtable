/*
 * Copyright 2017, OpenCensus Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opencensus.examples.bigtable;

import static com.google.common.base.Preconditions.checkState;
import static io.opencensus.contrib.grpc.metrics.RpcMeasureConstants.RPC_CLIENT_ROUNDTRIP_LATENCY;
import static io.opencensus.contrib.grpc.metrics.RpcMeasureConstants.RPC_METHOD;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.util.TracingUtilities;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.opencensus.common.Duration;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcViewConstants;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import io.opencensus.stats.View.AggregationWindow.Cumulative;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A minimal application that connects to Cloud Bigtable using the native HBase API and performs
 * some basic operations.
 */
public class DemoApp {
  private static final Tracer tracer = Tracing.getTracer();
  private static final Tagger tagger = Tags.getTagger();
  private static final TagKey SPECIAL_KEY = TagKey.create("MySpecialDimension");

  // Common histogram bucket boundaries for latency and elapsed-time Views.
  static final List<Double> RPC_MILLIS_BUCKET_BOUNDARIES = Collections.unmodifiableList(
      Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 13.0, 16.0, 20.0, 25.0, 30.0,
          40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0, 250.0, 300.0, 400.0, 500.0, 650.0,
          800.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0, 100000.0));

  // RPC client interval views.
  public static final View RPC_CLIENT_WITH_SPECIAL =
      View.create(
          View.Name.create("demo.io/client/roundtrip_latency_special/cumulative"),
          "Minute stats for latency in msecs",
          RPC_CLIENT_ROUNDTRIP_LATENCY,
          Distribution.create(BucketBoundaries.create(RPC_MILLIS_BUCKET_BOUNDARIES)),
          Arrays.asList(RPC_METHOD, SPECIAL_KEY),
          Cumulative.create());

  // Refer to table metadata names by byte array in the HBase API
  private static final byte[] TABLE_NAME = Bytes.toBytes("BigtableDemoApp");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("demo_column_family");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("demo_column_name");
  private static final String VALUE_PREFIX = "demo_value_";
  private static final String ROW_KEY_PREFIX = "demo_row_";
  private static final Sampler SAMPLER = Samplers.probabilitySampler(0.1);

  private static final Set<View> RPC_VIEW_SET =
      ImmutableSet.of(
          RpcViewConstants.RPC_CLIENT_ERROR_COUNT_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_ERROR_COUNT_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_ERROR_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_COUNT_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_COUNT_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_COUNT_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_COUNT_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_ROUNDTRIP_LATENCY_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_ROUNDTRIP_LATENCY_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_ROUNDTRIP_LATENCY_VIEW,
          RpcViewConstants.RPC_CLIENT_STARTED_COUNT_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_STARTED_COUNT_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_FINISHED_COUNT_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_FINISHED_COUNT_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_BYTES_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_BYTES_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_BYTES_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_BYTES_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_REQUEST_BYTES_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_REQUEST_BYTES_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_RESPONSE_BYTES_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_RESPONSE_BYTES_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_SERVER_ELAPSED_TIME_HOUR_VIEW,
          RpcViewConstants.RPC_CLIENT_SERVER_ELAPSED_TIME_MINUTE_VIEW,
          RpcViewConstants.RPC_CLIENT_SERVER_ELAPSED_TIME_VIEW);

  // Creates a table with a single column family
  private static void createTable(Admin admin, byte[] tableName, byte[] familyName) {
    try (Scope scope =
        tracer.spanBuilder("DemoCreateTable").setSampler(SAMPLER).startScopedSpan()) {
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      descriptor.addFamily(new HColumnDescriptor(familyName));
      try {
        admin.createTable(descriptor);
      } catch (IOException e) {
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
      }
    }
  }

  // Cleans up by disabling and then deleting the table
  private static void cleanUpAndDeleteTable(Admin admin, byte[] tableName) {
    try (Scope scope =
        tracer.spanBuilder("DemoCleanUpAndDeleteTable").setSampler(SAMPLER).startScopedSpan()) {
      try {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
      } catch (IOException e) {
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
      }
    }
  }

  private static void put(Table table, byte[] rowKey, byte[] value) {
    try (Scope scope_tag =
        tagger
            .currentBuilder()
            .put(
                SPECIAL_KEY,
                (value[0] & (byte) 1) == (byte) 1
                    ? TagValue.create("Even")
                    : TagValue.create("Odd"))
            .buildScoped()) {
      try (Scope scope_span = tracer.spanBuilder("DemoPut").setSampler(SAMPLER).startScopedSpan()) {
        try {
          // Put a single row into the table. We could also pass a list of Puts to write a batch.
          Put put = new Put(rowKey);
          put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, value);
          table.put(put);
        } catch (IOException e) {
          tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
        }
      }
    }
  }

  private static void get(Table table, byte[] rowKey, byte[] expectedValue) {

    try (Scope scope = tracer.spanBuilder("DemoGet").setSampler(SAMPLER).startScopedSpan()) {
      try {
        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Result getResult = table.get(new Get(rowKey));
        byte[] actualValue = getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        if (!Arrays.equals(expectedValue, actualValue)) {
          tracer
              .getCurrentSpan()
              .setStatus(
                  Status.UNKNOWN.withDescription(
                      "Expected value: "
                          + Bytes.toString(expectedValue)
                          + " got value: "
                          + Bytes.toString(actualValue)));
        }
      } catch (IOException e) {
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
      }
    }
  }

  /** Connects to Cloud Bigtable, runs some basic operations and prints the results. */
  private static void doHelloWorld(String projectId, String instanceId) {
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      // The admin API lets us create, manage and delete tables
      Admin admin = connection.getAdmin();
      cleanUpAndDeleteTable(admin, TABLE_NAME);
      createTable(admin, TABLE_NAME, COLUMN_FAMILY_NAME);

      // Retrieve the table we just created so we can do some reads and writes
      Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

      // Main loop that writes then read values;
      int rowIndex = 0;
      int valueIntex = 0;
      while (!Thread.interrupted()) {
        byte[] rowKey = Bytes.toBytes(ROW_KEY_PREFIX + rowIndex++);
        if (rowIndex == 17) {
          rowIndex = 0;
        }
        byte[] value = Bytes.toBytes(VALUE_PREFIX + valueIntex++);
        if (valueIntex == 27) {
          valueIntex = 0;
        }
        put(table, rowKey, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        get(table, rowKey, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }

  private static void registerViews() {
    for (View view : RPC_VIEW_SET) {
      Stats.getViewManager().registerView(view);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    // Consult system properties to get project/instance
    String projectId = "e2e-debugging"; // requiredProperty("bigtable.projectID");
    String instanceId = "e2e-debugging"; // requiredProperty("bigtable.instanceID");
    int portNumber = 8080; // Integer.getInteger(requiredProperty("bigtable.portNumber"));

    TracingUtilities.setupTracingConfig();
    // Still need to register span names for gRPC spans until the stubs are re-generated using
    // gRPC 1.8.
    Tracing.getExportComponent()
        .getSampledSpanStore()
        .registerSpanNamesForCollection(
            Arrays.asList(
                "DemoCreateTable",
                "DemoCleanUpAndDeleteTable",
                "DemoGet",
                "DemoPut",
                "Sent.google.devtools.cloudtrace.v2.TraceService.BatchWriteSpans",
                "Sent.google.monitoring.v3.MetricService.CreateMetricDescriptor",
                "Sent.google.monitoring.v3.MetricService.CreateTimeSeries"));

    // This needs to be done for the moment by all users.
    // registerViews();

    Stats.getViewManager().registerView(RPC_CLIENT_WITH_SPECIAL);

    // StackdriverExporter.createAndRegisterWithProjectId(projectId);
    StackdriverStatsExporter.createAndRegisterWithProjectId(projectId, Duration.create(5, 0));
    ZPageHandlers.startHttpServerAndRegisterAll(portNumber);

    doHelloWorld(projectId, instanceId);
  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    checkState(value != null, "Missing required system property: " + prop);
    return value;
  }
}
