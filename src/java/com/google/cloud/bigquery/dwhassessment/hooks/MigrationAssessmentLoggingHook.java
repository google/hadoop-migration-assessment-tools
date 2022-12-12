/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigquery.dwhassessment.hooks;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.EventLogger;
import java.time.Clock;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hook, which captures query events and stores them as {@link GenericRecord}. */
public class MigrationAssessmentLoggingHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(MigrationAssessmentLoggingHook.class);

  public void run(HookContext hookContext) throws Exception {
    try {
      EventLogger logger = EventLogger.getInstance(hookContext.getConf(), Clock.systemUTC());
      logger.handle(hookContext);
    } catch (Exception e) {
      LOG.error("Got exception while processing event", e);
    }
  }
}
