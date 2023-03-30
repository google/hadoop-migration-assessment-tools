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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        targetDirectoryPath, conf, QUERY_EVENT_SCHEMA, fixedClock, TEST_ID);

    // Assert
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
  }

  @Test
  public void getWriter_createsRecordWriterForCurrentDateDirectory() throws Exception {
    LocalDate targetDate = DEFAULT_TIMESTAMP.atOffset(ZoneOffset.UTC).toLocalDate();
    Path targetDirectoryPath = new Path(tmpFolder, targetDate.toString());
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    boolean existedBefore = fs.exists(targetDirectoryPath);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(Clock.fixed(DEFAULT_TIMESTAMP, ZoneOffset.UTC));

    // Act
    RecordsWriter writer = datePartitionedRecordsWriterFactory.createWriter();

    // Assert
    assertThat(writer.getPath())
        .isEqualTo(
            new Path(
                targetDirectoryPath.getFileSystem(conf).resolvePath(targetDirectoryPath),
                "dwhassessment_2022-12-25T120022.3344_a665f132.avro"));
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
  }

  @Test
  public void getWriter_updatesDirectoryWithRollover() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(DEFAULT_TIMESTAMP);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);
    // Act
    RecordsWriter writer1 = datePartitionedRecordsWriterFactory.createWriter();
    clock.tick(Duration.ofMinutes(20));
    RecordsWriter writer2 = datePartitionedRecordsWriterFactory.createWriter();
    clock.tick(Duration.ofDays(1));
    datePartitionedRecordsWriterFactory.maybeUpdateRolloverTime();
    RecordsWriter writer3 = datePartitionedRecordsWriterFactory.createWriter();

    // Assert
    assertThat(writer1.getPath().getName()).isEqualTo(createExpectedFileName(DEFAULT_TIMESTAMP));
    Instant secondRecordInstant = parseDateTime("2022-12-25T12:20:22.3344Z");
    assertThat(writer2.getPath().getName()).isEqualTo(createExpectedFileName(secondRecordInstant));
    assertThat(writer2.getPath().getParent().getName()).isEqualTo("2022-12-25");
    Instant thirdRecordInstant = parseDateTime("2022-12-26T12:20:22.3344Z");
    assertThat(writer3.getPath().getName()).isEqualTo(createExpectedFileName(thirdRecordInstant));
    assertThat(writer3.getPath().getParent().getName()).isEqualTo("2022-12-26");
  }

  @Test
  public void shouldRollover_returnsFalseForSameDate() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(parseDateTime("2022-12-25T12:00:00.00Z"));
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);

    // Act
    clock.tick(Duration.ofHours(11));
    boolean shouldRollover = datePartitionedRecordsWriterFactory.shouldRollover();

    // Assert
    assertThat(shouldRollover).isFalse();
  }

  @Test
  public void shouldRollover_returnsTrueForNextDate() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(parseDateTime("2022-12-25T12:00:00.00Z"));
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);

    // Act
    clock.tick(Duration.ofHours(20));
    boolean shouldRollover = datePartitionedRecordsWriterFactory.shouldRollover();

    // Assert
    assertThat(shouldRollover).isTrue();
  }

  @Test
  public void shouldRollover_returnsTrueForNextDateOnTheEdge() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(parseDateTime("2022-12-25T23:59:59.00Z"));
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);

    // Act
    clock.tick(Duration.ofSeconds(2));
    boolean shouldRollover = datePartitionedRecordsWriterFactory.shouldRollover();

    // Assert
    assertThat(shouldRollover).isTrue();
  }

  @Test
  public void maybeUpdateRolloverTime_ignoresIfOnTheSameDate() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(parseDateTime("2022-12-25T12:23:54.00Z"));
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);
    String currentDirectory =
        datePartitionedRecordsWriterFactory.createWriter().getPath().getParent().getName();

    // Act
    clock.tick(Duration.ofHours(3));
    boolean isUpdated = datePartitionedRecordsWriterFactory.maybeUpdateRolloverTime();

    // Assert
    assertThat(isUpdated).isFalse();
    assertThat(currentDirectory).isEqualTo("2022-12-25");
    assertThat(getCurrentWriterFolder(datePartitionedRecordsWriterFactory)).isEqualTo("2022-12-25");
  }

  @Test
  public void maybeUpdateRolloverTime_updatesWhenOnTheDifferentDate() throws Exception {
    TickableFixedClock clock = new TickableFixedClock(parseDateTime("2022-12-25T12:23:54.00Z"));
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clock);
    String currentDirectory =
        datePartitionedRecordsWriterFactory.createWriter().getPath().getParent().getName();

    // Act
    clock.tick(Duration.ofHours(20));
    boolean isUpdated = datePartitionedRecordsWriterFactory.maybeUpdateRolloverTime();

    // Assert
    assertThat(isUpdated).isTrue();
    assertThat(currentDirectory).isEqualTo("2022-12-25");
    assertThat(getCurrentWriterFolder(datePartitionedRecordsWriterFactory)).isEqualTo("2022-12-26");
  }

  @Test
  public void getDateFromDir_success() {
    LocalDate expected = LocalDate.of(2022, 12, 8);

    assertThat(DatePartitionedRecordsWriterFactory.getDateFromDir("2022-12-08"))
        .isEqualTo(expected);
  }

  @Test
  public void getDateFromDir_invalidDirectoryName_fail() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatePartitionedRecordsWriterFactory.getDateFromDir("test"));

    assertThat(e).hasMessageThat().isEqualTo("Invalid directory: test");
  }

  private DatePartitionedRecordsWriterFactory createRecordsWriterFactory(Clock clock)
      throws IOException {
    return new DatePartitionedRecordsWriterFactory(
        new Path(tmpFolder), conf, QUERY_EVENT_SCHEMA, clock, TEST_ID);
  }

  private String createExpectedFileName(Instant instant) {
    return "dwhassessment_" + instant.toEpochMilli() + "_" + TEST_ID + ".avro";
  }

  private static Instant parseDateTime(String dateTimeString) {
    return Instant.parse(dateTimeString);
  }

  private String getCurrentWriterFolder(DatePartitionedRecordsWriterFactory recordsWriterFactory)
      throws IOException {
    return recordsWriterFactory.createWriter().getPath().getParent().getName();
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
