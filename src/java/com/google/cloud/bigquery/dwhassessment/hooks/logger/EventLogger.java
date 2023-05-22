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

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_BASE_PATH;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_QUEUE_CAPACITY;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_ROLLOVER_INTERVAL;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidator.getHiveVersion;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidator.isHiveVersionSupported;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.IdGenerator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main handler of query events. Processes the captured events and writes resulting records to a
 * file.
 */
public class EventLogger {
  private static final Logger LOG = LoggerFactory.getLogger(EventLogger.class);

  private static final int MAX_RETRIES = 2;
  private static final Duration SHUTDOWN_WAIT_TIME = Duration.ofSeconds(5);
  private static final int QUERY_EVENTS_QUEUE_DEFAULT_SIZE = 64;
  private static final Duration DEFAULT_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL_DURATION =
      Duration.ofMinutes(10);
  private static final Duration DEFAULT_ROLLOVER_INTERVAL_DURATION = Duration.ofMinutes(10);

  private final DatePartitionedRecordsWriterFactory recordsWriterFactory;
  private final EventRecordConstructor eventRecordConstructor;
  private final ScheduledThreadPoolExecutor logWriter;
  private final int queueCapacity;
  private final String loggerId;

  // Singleton using DCL.
  private static volatile EventLogger instance;

  public static EventLogger getInstance(HiveConf conf, Clock clock) {
    if (instance == null) {
      synchronized (EventLogger.class) {
        if (instance == null) {
          instance = new EventLogger(conf, clock);
          ShutdownHookManager.addShutdownHook(instance::shutdown);
        }
      }
    }
    return instance;
  }

  protected EventLogger(HiveConf conf, Clock clock) {
    eventRecordConstructor = new EventRecordConstructor(clock);
    loggerId = IdGenerator.generate();
    queueCapacity =
        conf.getInt(
            HIVE_QUERY_EVENTS_QUEUE_CAPACITY.getConfName(), QUERY_EVENTS_QUEUE_DEFAULT_SIZE);

    String baseDir = conf.get(HIVE_QUERY_EVENTS_BASE_PATH.getConfName());

    if (StringUtils.isBlank(baseDir)) {
      baseDir = null;
      LOG.error(
          "Log dir configuration key '{}' is not set, logging disabled.",
          HIVE_QUERY_EVENTS_QUEUE_CAPACITY.getConfName());
    }

    recordsWriterFactory = createRecordsWriterFactory(baseDir, conf, clock);
    if (!isHiveVersionSupported(getHiveVersion()) || recordsWriterFactory == null) {
      logWriter = null;
      return;
    }

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Migration Assessment Hook Query Log Writer %d")
            .build();
    logWriter = new ScheduledThreadPoolExecutor(1, threadFactory);

    long rolloverEligibilityIntervalMilliseconds =
        conf.getTimeDuration(
            HIVE_QUERY_EVENTS_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL.getConfName(),
            DEFAULT_ROLLOVER_ELIGIBILITY_CHECK_INTERVAL_DURATION.toMillis(),
            TimeUnit.MILLISECONDS);
    logWriter.scheduleWithFixedDelay(
        this::handleTick,
        rolloverEligibilityIntervalMilliseconds,
        rolloverEligibilityIntervalMilliseconds,
        TimeUnit.MILLISECONDS);

    LOG.info(
        "Logger successfully started, waiting for query events. Log directory is '{}'", baseDir);
  }

  public void handle(HookContext hookContext) {
    if (recordsWriterFactory == null) {
      return;
    }
    // Note: same hookContext object is used for all the events for a given query, if we try to
    // do it async we have concurrency issues and when query cache is enabled, post event comes
    // before we start the pre hook processing and causes inconsistent events publishing.
    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      LOG.debug("Received null query plan.");
      return;
    }

    Optional<GenericRecord> maybeEvent = eventRecordConstructor.constructEvent(hookContext);

    maybeEvent.ifPresent(event -> tryWriteEvent(event, hookContext.getHookType()));
  }

  private void tryWriteEvent(GenericRecord event, HookType hookType) {
    try {
      // ScheduledThreadPoolExecutor uses an unbounded queue which cannot be replaced with a
      // bounded queue.
      // Therefore, checking queue capacity manually here.
      if (logWriter.getQueue().size() < queueCapacity) {
        logWriter.execute(() -> writeEventWithRetries(event));
      } else {
        LOG.warn(
            "Writer queue full ignoring event {} for query {}", hookType, event.get("QueryId"));
      }
    } catch (RejectedExecutionException e) {
      LOG.warn("Writer queue full ignoring event {} for query {}", hookType, event.get("QueryId"));
    }
  }

  private DatePartitionedRecordsWriterFactory createRecordsWriterFactory(
      String baseDir, HiveConf conf, Clock clock) {
    if (baseDir == null) {
      return null;
    }

    Duration rolloverInterval =
        Duration.ofMillis(
            conf.getTimeDuration(
                HIVE_QUERY_EVENTS_ROLLOVER_INTERVAL.getConfName(),
                DEFAULT_ROLLOVER_INTERVAL_DURATION.toMillis(),
                TimeUnit.MILLISECONDS));

    try {
      return new DatePartitionedRecordsWriterFactory(
          new Path(baseDir), conf, QUERY_EVENT_SCHEMA, clock, loggerId, rolloverInterval);
    } catch (IOException e) {
      LOG.error("Unable to initialize logger, logging disabled.", e);
    }

    return null;
  }

  private synchronized void handleTick() {
    recordsWriterFactory.maybeRolloverWriter();
  }

  private synchronized void writeEventWithRetries(GenericRecord event) {
    for (int retryCount = 0; retryCount <= MAX_RETRIES; ++retryCount) {
      try {
        recordsWriterFactory.write(event);
        return;
      } catch (UncheckedIOException e) {
        reportRetryState(event, retryCount, e);
        waitForRetry(retryCount);
      }
    }
  }

  private void reportRetryState(
      GenericRecord event, int retryCount, UncheckedIOException writeException) {
    if (retryCount < MAX_RETRIES) {
      LOG.warn(
          "Error writing proto message for query {}, eventType: {}, retryCount: {}",
          event.get("QueryId"),
          event.get("EventType"),
          retryCount,
          writeException);
      LOG.trace("Exception", writeException);
    } else {
      LOG.error(
          "Error writing proto message for query {}, eventType: {}",
          event.get("QueryId"),
          event.get("EventType"),
          writeException);
    }
  }

  private void waitForRetry(int retryCount) {
    try {
      // 0 seconds, for first retry assuming fs object was closed and open will fix it.
      SECONDS.sleep((long) retryCount * retryCount);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Got interrupted in retry sleep.", e);
    }
  }

  public void shutdown() {
    if (logWriter != null) {
      logWriter.shutdown();
      try {
        logWriter.awaitTermination(SHUTDOWN_WAIT_TIME.getSeconds(), SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Got interrupted exception while waiting for events to be flushed", e);
      }
    }
    recordsWriterFactory.close();
  }
}
