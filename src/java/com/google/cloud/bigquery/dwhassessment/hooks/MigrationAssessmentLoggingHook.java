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
package com.google.cloud.bigquery.dwhassessment.hooks;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.LOGGER_NAME;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidator.getHiveVersion;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidator.isHiveVersionSupported;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.EventLogger;
import com.google.cloud.bigquery.dwhassessment.hooks.logger.exception.LoggingHookFatalException;
import java.io.File;
import java.time.Clock;
import java.util.Arrays;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hook, which captures query events and stores them as {@link GenericRecord}. */
public class MigrationAssessmentLoggingHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);

  private static final String version = getHiveVersion();

  public void run(HookContext hookContext) throws Exception {
    if (!isHiveVersionSupported(version)) {
      LOG.error(
          "Current Hive version '{}' is not supported by the Assessment logging hook, logging"
              + " disabled. Please refer to the documentation.",
          version);
      return;
    }

    try {
      EventLogger logger = EventLogger.getInstance(hookContext.getConf(), Clock.systemUTC());
      logger.handle(hookContext);
    } catch (Throwable e) {
      String baseMessage = "Got an exception while processing event.";
      // Handles errors such as NoSuchMethodError, NoClassDefFoundError
      if (e instanceof LinkageError | e instanceof LoggingHookFatalException) {
        String classpath =
            Arrays.toString(System.getProperty("java.class.path").split(File.pathSeparator));
        String errorMessage =
            baseMessage
                + " Please contact bq-edw-migration-support@google.com with this log data."
                + " Classpath is {}";

        LOG.error(errorMessage, classpath, e);
      } else {
        LOG.error(baseMessage, e);
      }
    }
  }
}
