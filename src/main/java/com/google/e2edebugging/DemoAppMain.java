package com.google.e2edebugging;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.e2edebugging.bigtable.BigtableOperations;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

public class DemoAppMain {
  // Refer to table metadata names by byte array in the HBase API
  private static final byte[] TABLE_NAME = Bytes.toBytes("BigtableDemoApp");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("bt_column_family");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("bt_column_name");
  private static final String VALUE_PREFIX = "bt_value_";
  private static final String ROW_KEY_PREFIX = "bt_row_";

  /** Connects to Cloud Bigtable, runs some basic operations. */
  private static void doBigtableOperations(String projectId, String instanceId) {
    // Create the Bigtable connection, use try-with-resources to make sure it gets closed
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      BigtableOperations bigtableOperations =
          BigtableOperations.create(connection, TABLE_NAME, COLUMN_FAMILY_NAME);
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
        bigtableOperations.put(rowKey, COLUMN_FAMILY_NAME, COLUMN_NAME, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        bigtableOperations.get(rowKey, COLUMN_FAMILY_NAME, COLUMN_NAME, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
      }
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }

  public static void main(String[] args) throws IOException {
    // Consult system properties to get project/instance
    String projectId = requiredProperty("bigtable.projectID");
    String instanceId = requiredProperty("bigtable.instanceID");
    int portNumber = Integer.getInteger(requiredProperty("zpages.portNumber"));

    // Still need to register span names for gRPC spans until the stubs are re-generated using
    // gRPC 1.8.
    Tracing.getExportComponent()
        .getSampledSpanStore()
        .registerSpanNamesForCollection(
            Arrays.asList(
                "Sent.google.devtools.cloudtrace.v2.TraceService.BatchWriteSpans",
                "Sent.google.monitoring.v3.MetricService.CreateMetricDescriptor",
                "Sent.google.monitoring.v3.MetricService.CreateTimeSeries"));

    // This needs to be done for the moment by all users.
    RpcViews.registerAllViews();

    StackdriverTraceExporter.createAndRegister(StackdriverTraceConfiguration.builder().build());
    StackdriverStatsExporter.createAndRegister(StackdriverStatsConfiguration.builder().build());
    ZPageHandlers.startHttpServerAndRegisterAll(portNumber);

    doBigtableOperations(projectId, instanceId);
  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    checkState(value != null, "Missing required system property: " + prop);
    return value;
  }
}
