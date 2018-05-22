/*
 * Copyright 2018, OpenCensus Authors
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

package com.google.e2edebugging.monitoring;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.collect.ImmutableSet;
import com.google.monitoring.v3.MetricDescriptorName;
import io.opencensus.contrib.grpc.metrics.RpcViewConstants;
import io.opencensus.stats.View;
import java.io.IOException;

public class MetricsDeleter {
  private static final ImmutableSet<View> RPC_CUMULATIVE_VIEWS_SET =
      ImmutableSet.of(
          RpcViewConstants.RPC_CLIENT_ERROR_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_ROUNDTRIP_LATENCY_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_REQUEST_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_RESPONSE_COUNT_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_UNCOMPRESSED_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_CLIENT_SERVER_ELAPSED_TIME_VIEW,
          RpcViewConstants.RPC_CLIENT_STARTED_COUNT_CUMULATIVE_VIEW,
          RpcViewConstants.RPC_CLIENT_FINISHED_COUNT_CUMULATIVE_VIEW,
          RpcViewConstants.RPC_SERVER_ERROR_COUNT_VIEW,
          RpcViewConstants.RPC_SERVER_SERVER_LATENCY_VIEW,
          RpcViewConstants.RPC_SERVER_SERVER_ELAPSED_TIME_VIEW,
          RpcViewConstants.RPC_SERVER_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_SERVER_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_SERVER_REQUEST_COUNT_VIEW,
          RpcViewConstants.RPC_SERVER_RESPONSE_COUNT_VIEW,
          RpcViewConstants.RPC_SERVER_UNCOMPRESSED_REQUEST_BYTES_VIEW,
          RpcViewConstants.RPC_SERVER_UNCOMPRESSED_RESPONSE_BYTES_VIEW,
          RpcViewConstants.RPC_SERVER_STARTED_COUNT_CUMULATIVE_VIEW,
          RpcViewConstants.RPC_SERVER_FINISHED_COUNT_CUMULATIVE_VIEW);

  public static void DeleteTable(String project, String name) throws IOException {
    MetricServiceClient metricServiceClient = MetricServiceClient.create();
    metricServiceClient.deleteMetricDescriptor(MetricDescriptorName.of(project, name));
  }

  public static void DeleteAllSDTables(String project) {
    for (View view : RPC_CUMULATIVE_VIEWS_SET) {
      try {
        DeleteTable(project, "custom.googleapis.com/opencensus/" + view.getName().asString());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
