package com.google.cloud.bigquery.dwhassessment.hooks.logger;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_BASE_PATH;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_QUEUE_CAPACITY;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.HIVE_QUERY_EVENTS_ROLLOVER_CHECK_INTERVAL;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.utils.IOUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
  private static final Duration DEFAULT_ROLLOVER_TIME_MILLISECONDS = Duration.ofSeconds(600);

  private final DatePartitionedRecordsWriterFactory logger;
  private final EventRecordConstructor eventRecordConstructor;
  private final ScheduledThreadPoolExecutor logWriter;
  private final int queueCapacity;
  private final UUID loggerId;

  private int logFileCount = 0;
  private RecordsWriter writer;
  private LocalDate writerDate;

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
    loggerId = UUID.randomUUID();
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

    logger = createLogger(baseDir, conf, clock);
    if (logger == null) {
      logWriter = null;
      return;
    }

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Migration Assessment Hook Query Log Writer %d")
            .build();
    logWriter = new ScheduledThreadPoolExecutor(1, threadFactory);

    long rolloverIntervalMilliseconds =
        conf.getTimeDuration(
            HIVE_QUERY_EVENTS_ROLLOVER_CHECK_INTERVAL.getConfName(),
            DEFAULT_ROLLOVER_TIME_MILLISECONDS.toMillis(),
            TimeUnit.MILLISECONDS);

    logWriter.scheduleWithFixedDelay(
        this::handleTick,
        rolloverIntervalMilliseconds,
        rolloverIntervalMilliseconds,
        TimeUnit.MILLISECONDS);
  }

  public void handle(HookContext hookContext) {
    if (logger == null) {
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

  private DatePartitionedRecordsWriterFactory createLogger(String baseDir, HiveConf conf, Clock clock) {
    if (baseDir == null) {
      return null;
    }

    try {
      return new DatePartitionedRecordsWriterFactory(new Path(baseDir), conf, QUERY_EVENT_SCHEMA, clock);
    } catch (IOException e) {
      LOG.error("Unable to initialize logger, logging disabled.", e);
    }

    return null;
  }

  private String constructFileName() {
    return "dwhassessment_" + loggerId + "_" + logFileCount + ".avro";
  }

  private void handleTick() {
    try {
      maybeRolloverWriterForDay();
    } catch (IOException e) {
      LOG.error("Got IOException while trying to rollover", e);
    }
  }

  private void maybeRolloverWriterForDay() throws IOException {
    if (writer == null || !logger.getNow().equals(writerDate)) {
      if (writer != null) {
        // Day changes over case, reset the logFileCount.
        logFileCount = 0;
        IOUtils.closeQuietly(writer);
        writer = null;
      }
      // increment log file count, if creating a new writer.
      ++logFileCount;
      writer = logger.createWriter(constructFileName());
      writerDate = DatePartitionedRecordsWriterFactory.getDateFromDir(writer.getPath().getParent().getName());
    }
  }

  private void writeEventWithRetries(GenericRecord event) {
    for (int retryCount = 0; retryCount <= MAX_RETRIES; ++retryCount) {
      try {
        maybeRolloverWriterForDay();
        writer.writeMessage(event);
        writer.flush();
        return;
      } catch (IOException e) {
        // Something wrong with writer – close and reopen.
        IOUtils.closeQuietly(writer);
        writer = null;
        reportRetryState(event, retryCount, e);
        waitForRetry(retryCount);
      }
    }
  }

  private void reportRetryState(GenericRecord event, int retryCount, IOException writeException) {
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
    IOUtils.closeQuietly(writer);
  }
}
