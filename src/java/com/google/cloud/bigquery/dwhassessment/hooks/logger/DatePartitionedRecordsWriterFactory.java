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

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.LOGGER_NAME;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENTS_FILE_PREFIX;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.apache.commons.lang.ObjectUtils.min;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for {@link RecordsWriter} instances. Manages them to write to files, partitioned by
 * dates.
 */
public class DatePartitionedRecordsWriterFactory {
  public static final DateTimeFormatter LOG_TIME_FORMAT =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(ISO_LOCAL_DATE)
          .appendLiteral('T')
          .appendValue(HOUR_OF_DAY, 2)
          .appendValue(MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendValue(SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 9, true)
          .toFormatter();

  private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);
  private static final FsPermission DIR_PERMISSION = FsPermission.createImmutable((short) 1023);

  private final Path basePath;
  private final Configuration conf;
  private final Schema schema;
  private final Clock clock;
  private Instant rolloverTime;
  private final String loggerId;
  private final Duration rolloverInterval;

  // Is constructed lazily on the first write call
  @Nullable private RecordsWriter currentWriter;

  public DatePartitionedRecordsWriterFactory(
      Path baseDir,
      Configuration conf,
      Schema schema,
      Clock clock,
      String loggerId,
      Duration rolloverInterval)
      throws IOException {
    this.conf = conf;
    this.createDirIfNotExists(baseDir);
    this.schema = schema;
    this.clock = clock;
    this.loggerId = loggerId;
    this.rolloverInterval = rolloverInterval;
    basePath = baseDir.getFileSystem(conf).resolvePath(baseDir);
    rolloverTime = calculateNextRolloverTime();
  }

  /** Attempts to write an event to a writer. Creates a writer if it is null. */
  public void write(GenericRecord event) throws UncheckedIOException {
    maybeRolloverWriter();

    try {
      if (currentWriter == null) {
        currentWriter = createWriter();
      }
      currentWriter.writeMessage(event);
      currentWriter.flush();
      LOG.debug("Wrote query '{}', event type '{}'", event.get("QueryId"), event.get("EventType"));
    } catch (IOException e) {
      // Something wrong with writer – close and reopen.
      close();

      throw new UncheckedIOException("Exception during writing query event", e);
    }
  }

  public void maybeRolloverWriter() {
    if (shouldRollover()) {
      close();

      rolloverTime = calculateNextRolloverTime();

      LOG.info(
          "Updated rollover time for logger ID '{}' to '{}'",
          loggerId,
          ISO_LOCAL_DATE_TIME.format(rolloverTime.atOffset(ZoneOffset.UTC)));
    } else {
      LOG.info(
          "Performed rollover check for logger ID '{}'. Expected rollover time is '{}'",
          loggerId,
          ISO_LOCAL_DATE_TIME.format(rolloverTime.atOffset(ZoneOffset.UTC)));
    }
  }

  public void close() {
    if (currentWriter != null) {
      try {
        currentWriter.close();
        LOG.info("Closed file '{}' for logger ID '{}'", currentWriter.getPath(), loggerId);
      } catch (IOException e) {
        LOG.error("Failed to close writer for file '{}'", currentWriter.getPath(), e);
      }
    }

    currentWriter = null;
  }

  public Duration getRolloverInterval() {
    return rolloverInterval;
  }

  /**
   * Creates new writer for the current date. Each new invocation increments the {@code
   * logFileCount}.
   */
  private RecordsWriter createWriter() throws IOException {
    String fileName = constructFileName();
    Path filePath = getPathForDate(getCurrentDate(), fileName);

    LOG.info("Creating new query events writer for file name '{}'", filePath.getName());

    return new RecordsWriter(conf, filePath, schema);
  }

  private boolean shouldRollover() {
    return clock.instant().isAfter(rolloverTime);
  }

  private void createDirIfNotExists(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(conf);

    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
        fileSystem.setPermission(path, DIR_PERMISSION);
      }
    } catch (IOException e) {
      LOG.warn("Error while trying to set permission", e);
    }
  }

  private Path getPathForDate(LocalDate date, String fileName) throws IOException {
    Path path = new Path(basePath, getDirForDate(date));
    createDirIfNotExists(path);
    return new Path(path, fileName);
  }

  private String getDirForDate(LocalDate date) {
    return ISO_LOCAL_DATE.format(date);
  }

  /**
   * Next rollover time is at next configured interval or at the beginning of the next day,
   * depending on what will happen earlier.
   */
  private Instant calculateNextRolloverTime() {
    Instant currentInstant = clock.instant();
    Instant nextRollover = currentInstant.plus(rolloverInterval).truncatedTo(ChronoUnit.MINUTES);
    Instant nextDay = currentInstant.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);

    return (Instant) min(nextRollover, nextDay);
  }

  private LocalDate getCurrentDate() {
    return clock.instant().atOffset(ZoneOffset.UTC).toLocalDate();
  }

  private String constructFileName() {
    return QUERY_EVENTS_FILE_PREFIX
        + LOG_TIME_FORMAT.format(clock.instant().atOffset(ZoneOffset.UTC))
        + "_"
        + loggerId
        + ".avro";
  }
}
