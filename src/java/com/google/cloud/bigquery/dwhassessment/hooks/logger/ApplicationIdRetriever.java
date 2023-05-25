/*
 * Copyright 2022-2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery.dwhassessment.hooks.logger;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper for retrieving YARN Application ID depending on the execution mode. */
public class ApplicationIdRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationIdRetriever.class);

  public static Optional<ApplicationId> determineApplicationId(
      HiveConf conf, ExecutionMode executionMode) {
    switch (executionMode) {
      case MR:
        return determineMapReduceApplicationId();
      case TEZ:
        return determineTezApplicationId();
      case LLAP:
        return determineLlapApplicationId(conf, executionMode);
      default:
        return Optional.empty();
    }
  }

  /**
   * Retrieves Application ID for the Tez application. Application can be reused, but one
   * application always have only one queue – if queue changes in the session, new application is
   * created.
   */
  private static Optional<ApplicationId> determineTezApplicationId() {
    try {
      return Optional.of(
          SessionState.get().getTezSession().getSession().getAppMasterApplicationId());
    } catch (NullPointerException e) {
      LOG.info("Failed to retrieve Application ID from Tez session");
      return Optional.empty();
    }
  }

  /**
   * Retrieves Application ID from the first MapReduce job as multiple MapReduce jobs created for a
   * single query are submitted to the same queue.
   */
  private static Optional<ApplicationId> determineMapReduceApplicationId() {
    return SessionState.get().getMapRedStats().values().stream()
        .map(MapRedStats::getJobId)
        .map(jobId -> TypeConverter.toYarn(JobID.forName(jobId)).getAppId())
        .findFirst();
  }

  /**
   * Retrieve Application ID for Llap daemon. They are long-living YARN applications, using the same
   * queue, so it should be relatively static.
   */
  public static Optional<ApplicationId> determineLlapApplicationId(
      HiveConf conf, ExecutionMode mode) {
    // Note: for now, LLAP is only supported in Tez tasks. Will never come to MR; others may
    // be added here, although this is only necessary to have extra debug information.
    if (mode == ExecutionMode.LLAP) {
      // In HS2, the client should have been cached already for the common case.
      // Otherwise, this may actually introduce delay to compilation for the first query.
      String hosts = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
      if (hosts != null && !hosts.isEmpty()) {
        try {
          return Optional.of(LlapRegistryService.getClient(conf).getApplicationId());
        } catch (IOException e) {
          LOG.error("Error trying to get llap instance. Hosts: {}", hosts, e);
        }
      } else {
        LOG.info("Cannot determine LLAP instance on client - service hosts are not set");
      }
    }

    return Optional.empty();
  }
}
