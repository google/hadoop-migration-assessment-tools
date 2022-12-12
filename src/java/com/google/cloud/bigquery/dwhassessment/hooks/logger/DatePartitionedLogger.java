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
package com.google.cloud.bigquery.dwhassessment.hooks.logger;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes query events to files, partitioned by dates. */
public class DatePartitionedLogger {
  private static final Logger LOG = LoggerFactory.getLogger(DatePartitionedLogger.class);
  private static final FsPermission DIR_PERMISSION = FsPermission.createImmutable((short) 1023);
  private final Path basePath;
  private final Configuration conf;
  private final Schema schema;
  private final Clock clock;

  public DatePartitionedLogger(Path baseDir, Configuration conf, Schema schema, Clock clock)
      throws IOException {
    this.conf = conf;
    this.createDirIfNotExists(baseDir);
    this.basePath = baseDir.getFileSystem(conf).resolvePath(baseDir);
    this.schema = schema;
    this.clock = clock;
  }

  public static LocalDate getDateFromDir(String dirName) {
    if (!dirName.startsWith("date=")) {
      throw new IllegalArgumentException("Invalid directory: " + dirName);
    } else {
      return LocalDate.parse(dirName.substring(5), DateTimeFormatter.ISO_LOCAL_DATE);
    }
  }

  public RecordsWriter getWriter(String fileName) throws IOException {
    Path filePath = this.getPathForDate(this.getNow(), fileName);
    return new RecordsWriter(this.conf, filePath, schema);
  }

  private void createDirIfNotExists(Path path) throws IOException {
    FileSystem fileSystem = path.getFileSystem(this.conf);

    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
        fileSystem.setPermission(path, DIR_PERMISSION);
      }
    } catch (IOException e) {
      LOG.warn("Error while trying to set permission: ", e);
    }
  }

  private Path getPathForDate(LocalDate date, String fileName) throws IOException {
    Path path = new Path(this.basePath, this.getDirForDate(date));
    this.createDirIfNotExists(path);
    return new Path(path, fileName);
  }

  private String getDirForDate(LocalDate date) {
    return "date=" + DateTimeFormatter.ISO_LOCAL_DATE.format(date);
  }

  public LocalDate getNow() {
    return clock.instant().atOffset(ZoneOffset.UTC).toLocalDate();
  }
}
