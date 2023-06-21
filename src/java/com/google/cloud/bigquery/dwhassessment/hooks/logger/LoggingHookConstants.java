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

import com.google.cloud.bigquery.dwhassessment.hooks.avro.AvroSchemaLoader;
import java.time.Duration;
import org.apache.avro.Schema;

public final class LoggingHookConstants {

  public static final Schema QUERY_EVENT_SCHEMA = AvroSchemaLoader.loadSchema("QueryEvents.avsc");

  public static final String HOOK_VERSION = "1.0";
  
  public static final String QUERY_EVENTS_FILE_PREFIX = "dwhassessment_";

  public static final Duration DEFAULT_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL_DURATION =
      Duration.ofMinutes(10);
  public static final Duration DEFAULT_ROLLOVER_INTERVAL_DURATION = Duration.ofHours(1);

  private LoggingHookConstants() {}
}
