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

package com.google.e2edebugging.bigtable;

import com.google.cloud.bigtable.util.TracingUtilities;
import io.opencensus.common.Scope;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.Arrays;
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
public class BigtableOperations {

  private static final Tracer tracer = Tracing.getTracer();
  private static final String CREATE_TABLE_SPAN_NAME = "BigtableOperationsCreateTable";
  private static final String DELETE_TABLE_SPAN_NAME = "BigtableOperationsDeleteTable";
  private static final String GET_SPAN_NAME = "BigtableOperationsGet";
  private static final String PUT_SPAN_NAME = "BigtableOperationsPut";

  private final Table table;

  static {
    TracingUtilities.setupTracingConfig();
    Tracing.getExportComponent()
        .getSampledSpanStore()
        .registerSpanNamesForCollection(
            Arrays.asList(
                CREATE_TABLE_SPAN_NAME, DELETE_TABLE_SPAN_NAME, GET_SPAN_NAME, PUT_SPAN_NAME));
  }

  /**
   * Creates a new table with the given name and column family name.
   *
   * <p>If a table with the same name existed before that table is deleted.
   */
  public static BigtableOperations create(
      Connection connection, byte[] tableName, byte[] column_family_name) throws IOException {

    // The admin API lets us create, manage and delete tables
    Admin admin = connection.getAdmin();
    deleteTable(admin, tableName);
    createTable(admin, tableName, column_family_name);

    // Retrieve the table we just created so we can do reads and writes
    return new BigtableOperations(connection.getTable(TableName.valueOf(tableName)));
  }

  private BigtableOperations(Table table) {
    this.table = table;
  }

  // Creates a table with a single column family
  private static void createTable(Admin admin, byte[] tableName, byte[] familyName) {
    try (Scope scope =
        tracer
            .spanBuilder(CREATE_TABLE_SPAN_NAME)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
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
  private static void deleteTable(Admin admin, byte[] tableName) {
    try (Scope scope =
        tracer
            .spanBuilder(DELETE_TABLE_SPAN_NAME)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      try {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
      } catch (IOException e) {
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
      }
    }
  }

  public void put(byte[] rowKey, byte[] column_family_name, byte[] column_name, byte[] value) {
    try (Scope scope =
        tracer.spanBuilder(PUT_SPAN_NAME).setSampler(Samplers.alwaysSample()).startScopedSpan()) {
      try {
        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Put put = new Put(rowKey);
        put.addColumn(column_family_name, column_name, value);
        table.put(put);
      } catch (IOException e) {
        tracer.getCurrentSpan().setStatus(Status.UNKNOWN.withDescription(e.getMessage()));
      }
    }
  }

  public void get(
      byte[] rowKey, byte[] column_family_name, byte[] column_name, byte[] expectedValue) {
    try (Scope scope =
        tracer.spanBuilder(GET_SPAN_NAME).setSampler(Samplers.alwaysSample()).startScopedSpan()) {
      try {
        // Put a single row into the table. We could also pass a list of Puts to write a batch.
        Result getResult = table.get(new Get(rowKey));
        byte[] actualValue = getResult.getValue(column_family_name, column_name);
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
}
