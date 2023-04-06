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

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class DatePartitionedRecordsWriterFactoryTest {
  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private static final String TEST_ID = "a665f132";
  private static final Instant DEFAULT_TIMESTAMP = parseDateTime("2022-12-25T12:00:22.3344Z");

  private HiveConf conf;
  private String tmpFolder;

  @Before
  public void setup() throws IOException {
    conf = new HiveConf();
    tmpFolder = folder.newFolder().getAbsolutePath();
  }

  @Test
  public void constructor_createsDirectoryIfNotExists() throws Exception {
    Clock fixedClock = Clock.fixed(DEFAULT_TIMESTAMP, ZoneOffset.UTC);
    Path targetDirectoryPath = new Path(tmpFolder, "test_directory");
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    boolean existedBefore = fs.exists(targetDirectoryPath);

    // Act
    new DatePartitionedRecordsWriterFactory(
        targetDirectoryPath, conf, QUERY_EVENT_SCHEMA, fixedClock, TEST_ID, Duration.ofMinutes(30));

    // Assert
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
    assertThat(getDirectoryFiles(fs, targetDirectoryPath)).isEmpty();
  }

  @Test
  public void constructor_doesNotCreateOutputFilesIfNothingWritten() throws Exception {
    Path targetDirectoryPath = new Path(tmpFolder);
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(new TickableFixedClock(DEFAULT_TIMESTAMP));

    // Act
    datePartitionedRecordsWriterFactory.close();

    // Assert
    assertThat(getDirectoryFiles(fs, targetDirectoryPath)).isEmpty();
  }

  @Test
  public void getWriter_writeEventsToTheSameWriterIfWithinRolloverWindow() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(DEFAULT_TIMESTAMP);
    LocalDate targetDate = DEFAULT_TIMESTAMP.atOffset(ZoneOffset.UTC).toLocalDate();
    Path targetDirectoryPath = new Path(tmpFolder, targetDate.toString());
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);
    String expectedFileName = "dwhassessment_2022-12-25T120022.3344_a665f132.avro";

    // Act
    datePartitionedRecordsWriterFactory.write(newRecord("id1"));
    clock.tick(Duration.ofMinutes(10));
    datePartitionedRecordsWriterFactory.write(newRecord("id2"));
    datePartitionedRecordsWriterFactory.close();

    // Assert
    assertThat(getDirectoryFiles(fs, targetDirectoryPath)).containsExactly(expectedFileName);
    assertThat(readRecords(new Path(targetDirectoryPath, expectedFileName)))
        .containsExactly(newRecord("id1"), newRecord("id2"));
  }

  @Test
  public void getWriter_createsNewWriterIfShouldRollover() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(DEFAULT_TIMESTAMP);
    LocalDate targetDate = DEFAULT_TIMESTAMP.atOffset(ZoneOffset.UTC).toLocalDate();
    Path targetDirectoryPath = new Path(tmpFolder, targetDate.toString());
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);

    String fileName1 = createExpectedFileName(DEFAULT_TIMESTAMP);
    String fileName2 = createExpectedFileName(parseDateTime("2022-12-25T12:31:22.3344Z"));
    GenericRecord record1 = newRecord("id1");
    GenericRecord record2 = newRecord("id2");

    // Act
    datePartitionedRecordsWriterFactory.write(record1);
    clock.tick(Duration.ofMinutes(31));
    datePartitionedRecordsWriterFactory.write(record2);
    datePartitionedRecordsWriterFactory.close();

    // Assert
    assertThat(getDirectoryFiles(fs, targetDirectoryPath)).containsExactly(fileName1, fileName2);
    assertThat(readRecords(new Path(targetDirectoryPath, fileName1))).containsExactly(record1);
    assertThat(readRecords(new Path(targetDirectoryPath, fileName2))).containsExactly(record2);
  }

  @Test
  public void getWriter_updatesDirectoryWhenDayChanges() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(DEFAULT_TIMESTAMP);
    FileSystem fs = new Path(tmpFolder).getFileSystem(conf);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);

    // Act
    datePartitionedRecordsWriterFactory.write(newRecord("id1"));
    clock.tick(Duration.ofDays(1));
    datePartitionedRecordsWriterFactory.write(newRecord("id2"));
    datePartitionedRecordsWriterFactory.close();

    // Assert
    assertThat(getDirectoryFiles(fs, new Path(tmpFolder, "2022-12-25")))
        .containsExactly(createExpectedFileName(DEFAULT_TIMESTAMP));
    assertThat(getDirectoryFiles(fs, new Path(tmpFolder, "2022-12-26")))
        .containsExactly(createExpectedFileName(parseDateTime("2022-12-26T12:00:22.3344Z")));
  }

  private DatePartitionedRecordsWriterFactory createRecordsWriterFactory(Clock clock)
      throws IOException {
    return new DatePartitionedRecordsWriterFactory(
        new Path(tmpFolder), conf, QUERY_EVENT_SCHEMA, clock, TEST_ID, Duration.ofMinutes(30));
  }

  private String createExpectedFileName(Instant instant) {
    return "dwhassessment_"
        + DatePartitionedRecordsWriterFactory.LOG_TIME_FORMAT.format(
            instant.atOffset(ZoneOffset.UTC))
        + "_"
        + TEST_ID
        + ".avro";
  }

  private static Instant parseDateTime(String dateTimeString) {
    return Instant.parse(dateTimeString);
  }

  private static ImmutableList<String> getDirectoryFiles(FileSystem fs, Path parent)
      throws IOException {
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(parent, true);

    ImmutableList.Builder<String> fileNames = ImmutableList.builder();
    while (files.hasNext()) {
      fileNames.add(files.next().getPath().getName());
    }

    return fileNames.build();
  }

  private ImmutableList<GenericRecord> readRecords(Path avroPath) {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<>(new File(avroPath.toString()), datumReader)) {
      return ImmutableList.copyOf(dataFileReader.iterator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private GenericRecord newRecord(String queryId) {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA).set("QueryId", queryId).build();
  }

  /** Similar to {@link Clock#fixed(Instant, ZoneId)}, but allows adding time in place. */
  static final class TickableFixedClock extends Clock {

    private Instant instant;

    TickableFixedClock(Instant instant) {
      this.instant = instant;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return new TickableFixedClock(instant());
    }

    @Override
    public Instant instant() {
      return instant;
    }

    public void tick(Duration tickDuration) {
      instant = instant.plusNanos(tickDuration.toNanos());
    }
  }
}
