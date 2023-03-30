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

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import org.apache.avro.Schema;
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
  private static final Logger LOG =
      LoggerFactory.getLogger(DatePartitionedRecordsWriterFactory.class);
  private static final FsPermission DIR_PERMISSION = FsPermission.createImmutable((short) 1023);

  private static final DateTimeFormatter LOG_TIME_FORMAT =
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

  private final Path basePath;
  private final Configuration conf;
  private final Schema schema;
  private final Clock clock;
  private Instant rolloverTime;
  private final String loggerId;

  public DatePartitionedRecordsWriterFactory(
      Path baseDir, Configuration conf, Schema schema, Clock clock, String loggerId)
      throws IOException {
    this.conf = conf;
    this.createDirIfNotExists(baseDir);
    this.schema = schema;
    this.clock = clock;
    this.loggerId = loggerId;
    basePath = baseDir.getFileSystem(conf).resolvePath(baseDir);
    rolloverTime = calculateNextRolloverTime();
  }

  public static LocalDate getDateFromDir(String dirName) {
    try {
      return LocalDate.parse(dirName, ISO_LOCAL_DATE);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("Invalid directory: " + dirName, e);
    }
  }

  /**
   * Creates new writer for the current date. Each new invocation increments the {@code
   * logFileCount}.
   */
  public RecordsWriter createWriter() throws IOException {
    String fileName = constructFileName();
    Path filePath = getPathForDate(getCurrentDate(), fileName);
    return new RecordsWriter(conf, filePath, schema);
  }

  public boolean shouldRollover() {
    return clock.instant().isAfter(rolloverTime);
  }

  public boolean maybeUpdateRolloverTime() {
    if (!shouldRollover()) {
      LOG.debug(
          "Trying to update rollover time, but the current time is not after rollover time: {},"
              + " ignoring call",
          rolloverTime);
      return false;
    }

    rolloverTime = calculateNextRolloverTime();
    return true;
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

  private Instant calculateNextRolloverTime() {
    return clock.instant().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS);
  }

  private LocalDate getCurrentDate() {
    return clock.instant().atOffset(ZoneOffset.UTC).toLocalDate();
  }

  private String constructFileName() {
    return "dwhassessment_"
        + LOG_TIME_FORMAT.format(clock.instant().atOffset(ZoneOffset.UTC))
        + "_"
        + loggerId
        + ".avro";
  }
}
