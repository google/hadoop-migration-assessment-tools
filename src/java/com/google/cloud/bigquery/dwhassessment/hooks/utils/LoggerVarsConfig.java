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
package com.google.cloud.bigquery.dwhassessment.hooks.utils;

/** Stores configuration keys used by the hook. */
public enum LoggerVarsConfig {

  /** ConfName is equal to {@code org.apache.tez.dag.api.TezConfiguration.TEZ_QUEUE_NAME} */
  TEZ_QUEUE_NAME("tez.queue.name", "Name of the TEZ default queue"),
  /** ConfName is equal to {@code org.apache.hadoop.mapreduce.MRJobConfig.QUEUE_NAME} */
  MR_QUEUE_NAME("mapreduce.job.queuename", "Name of the MapReduce default queue"),
  HIVE_QUERY_EVENTS_QUEUE_CAPACITY(
      "dwhassessment.hook.queue.capacity",
      "Queue capacity for the query events logging threads, e.g. 100."),
  HIVE_QUERY_EVENTS_BASE_PATH(
      "dwhassessment.hook.base-directory",
      "Base directory for query event messages written by Migration Assessment hook."),

  HIVE_QUERY_EVENTS_ROLLOVER_INTERVAL(
      "dwhassessment.hook.rollover-interval",
      "Frequency at which the file rollover should be performed, e.g. 600s. On day change rollover"
          + " happens always."),

  HIVE_QUERY_EVENTS_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL(
      "dwhassessment.hook.rollover-eligibility-check-interval",
      "Frequency at which the file rollover eligibility check is triggered in the background, e.g."
          + " 600s.");

  private final String confName;
  private final String description;

  LoggerVarsConfig(String confName, String description) {
    this.confName = confName;
    this.description = description;
  }

  public String getConfName() {
    return confName;
  }

  public String getDescription() {
    return description;
  }
}
