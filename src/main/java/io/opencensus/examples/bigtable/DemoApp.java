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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.util.TracingUtilities;
import com.google.common.util.concurrent.Uninterruptibles;
import io.opencensus.common.Scope;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.Arrays;
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

  // Refer to table metadata names by byte array in the HBase API
  private static final byte[] TABLE_NAME = Bytes.toBytes("BigtableDemoApp");
  private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("demo_column_family");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("demo_column_name");
  private static final String VALUE_PREFIX = "demo_value_";
  private static final String ROW_KEY_PREFIX = "demo_row_";

  // Creates a table with a single column family
  private static void createTable(Admin admin, byte[] tableName, byte[] familyName) {
    try (Scope scope =
        tracer
            .spanBuilder("DemoCreateTable")
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      descriptor.addFamily(new HColumnDescriptor(familyName));
      try {
        admin.createTable(descriptor);
      } catch (IOException e) {
        // TODO(bdrutu): Change this to setStatus when move to opencensus 0.9.0.
        tracer
            .getCurrentSpan()
            .end(
                EndSpanOptions.builder()
                    .setStatus(Status.UNKNOWN.withDescription(e.getMessage()))
                    .build());
      }
    }
  }

  // Cleans up by disabling and then deleting the table
  private static void cleanUpAndDeleteTable(Admin admin, byte[] tableName) {
    try (Scope scope =
        tracer
            .spanBuilder("DemoCleanUpAndDeleteTable")
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      try {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
      } catch (IOException e) {
        // TODO(bdrutu): Change this to setStatus when move to opencensus 0.9.0.
        tracer
            .getCurrentSpan()
            .end(
                EndSpanOptions.builder()
                    .setStatus(Status.UNKNOWN.withDescription(e.getMessage()))
                    .build());
      }
    }
  }

  private static void put(Table table, byte[] rowKey, byte[] value) {
    try (Scope scope =
        tracer.spanBuilder("DemoPut").setSampler(Samplers.alwaysSample()).startScopedSpan()) {
      try {
        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Put put = new Put(rowKey);
        put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, value);
        table.put(put);
      } catch (IOException e) {
        // TODO(bdrutu): Change this to setStatus when move to opencensus 0.9.0.
        tracer
            .getCurrentSpan()
            .end(
                EndSpanOptions.builder()
                    .setStatus(Status.UNKNOWN.withDescription(e.getMessage()))
                    .build());
      }
    }
  }

  private static void get(Table table, byte[] rowKey, byte[] expectedValue) {
    try (Scope scope =
        tracer.spanBuilder("DemoGet").setSampler(Samplers.alwaysSample()).startScopedSpan()) {
      try {
        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Result getResult = table.get(new Get(rowKey));
        byte[] actualValue = getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
        if (!Arrays.equals(expectedValue, actualValue)) {
          // TODO(bdrutu): Change this to setStatus when move to opencensus 0.9.0.
          tracer
              .getCurrentSpan()
              .end(
                  EndSpanOptions.builder()
                      .setStatus(
                          Status.UNKNOWN.withDescription(
                              "Expected value: "
                                  + Bytes.toString(expectedValue)
                                  + " got value: "
                                  + Bytes.toString(actualValue)))
                      .build());
        }
      } catch (IOException e) {
        // TODO(bdrutu): Change this to setStatus when move to opencensus 0.9.0.
        tracer
            .getCurrentSpan()
            .end(
                EndSpanOptions.builder()
                    .setStatus(Status.UNKNOWN.withDescription(e.getMessage()))
                    .build());
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
        byte[] value = Bytes.toBytes(VALUE_PREFIX + valueIntex++);
        put(table, rowKey, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        get(table, rowKey, value);
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
        if (rowIndex == 17) {
          rowIndex = 0;
        }
        if (valueIntex == 27) {
          valueIntex = 0;
        }
      }

      cleanUpAndDeleteTable(admin, TABLE_NAME);
    } catch (IOException e) {
      System.err.println("Exception while running HelloWorld: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.exit(0);
  }

  private static void print(String msg) {
    System.out.println("HelloWorld: " + msg);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    // Consult system properties to get project/instance
    String projectId = requiredProperty("bigtable.projectID");
    String instanceId = requiredProperty("bigtable.instanceID");
    int portNumber = Integer.getInteger(requiredProperty("bigtable.portNumber"));

    TracingUtilities.setupTracingConfig();
    Tracing.getExportComponent()
        .getSampledSpanStore()
        .registerSpanNamesForCollection(
            Arrays.asList("DemoCreateTable", "DemoCleanUpAndDeleteTable", "DemoGet", "DemoPut"));

    StackdriverExporter.createAndRegisterWithProjectId(projectId);
    ZPageHandlers.startHttpServerAndRegisterAll(portNumber);

    doHelloWorld(projectId, instanceId);
  }

  private static String requiredProperty(String prop) {
    String value = System.getProperty(prop);
    checkState(value != null, "Missing required system property: " + prop);
    return value;
  }
}
