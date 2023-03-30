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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
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

  private static final String TEST_UUID = "a665f132-0606-4602-855d-04cd8a747f55";

  private HiveConf conf;
  private String tmpFolder;

  @Before
  public void setup() throws IOException {
    conf = new HiveConf();
    tmpFolder = folder.newFolder().getAbsolutePath();
  }

  @Test
  public void constructor_createsDirectoryIfNotExists() throws Exception {
    Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(123L), ZoneOffset.UTC);
    Path targetDirectoryPath = new Path(tmpFolder, "test_directory");
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    boolean existedBefore = fs.exists(targetDirectoryPath);

    // Act
    new DatePartitionedRecordsWriterFactory(
        targetDirectoryPath, conf, QUERY_EVENT_SCHEMA, fixedClock, UUID.randomUUID());

    // Assert
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
  }

  @Test
  public void getWriter_createsRecordWriterForCurrentDateDirectory() throws Exception {
    Instant fixedInstant = Instant.ofEpochMilli(1293285023000L);
    LocalDate targetDate = fixedInstant.atOffset(ZoneOffset.UTC).toLocalDate();
    Path targetDirectoryPath = new Path(tmpFolder, targetDate.toString());
    FileSystem fs = targetDirectoryPath.getFileSystem(conf);
    boolean existedBefore = fs.exists(targetDirectoryPath);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(Clock.fixed(fixedInstant, ZoneOffset.UTC));

    // Act
    RecordsWriter writer = datePartitionedRecordsWriterFactory.createWriter();

    // Assert
    assertThat(writer.getPath())
        .isEqualTo(
            new Path(
                targetDirectoryPath.getFileSystem(conf).resolvePath(targetDirectoryPath),
                "dwhassessment_" + TEST_UUID + "_1.avro"));
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
  }

  @Test
  public void getWriter_increasesLogCountWithEveryNewWriter() throws Exception {
    Instant fixedInstant = Instant.ofEpochMilli(1293285023000L);
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(Clock.fixed(fixedInstant, ZoneOffset.UTC));

    // Act
    RecordsWriter writer1 = datePartitionedRecordsWriterFactory.createWriter();
    RecordsWriter writer2 = datePartitionedRecordsWriterFactory.createWriter();
    RecordsWriter writer3 = datePartitionedRecordsWriterFactory.createWriter();

    // Assert
    assertThat(writer1.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_1.avro");
    assertThat(writer2.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_2.avro");
    assertThat(writer3.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_3.avro");
  }

  @Test
  public void getWriter_resetsLogCountWithRollover() throws Exception {
    Instant firstInstant = parseDateTime("2022-12-25T12:23:54.00Z");
    Instant currentInstant = parseDateTime("2022-12-26T15:00:00.00Z");
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(firstInstant, firstInstant, firstInstant) // Constructor and two RecordsWriters
        .thenReturn(currentInstant, currentInstant); // First date check and consecutive calls
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);

    // Act
    RecordsWriter writer1 = datePartitionedRecordsWriterFactory.createWriter();
    RecordsWriter writer2 = datePartitionedRecordsWriterFactory.createWriter();
    datePartitionedRecordsWriterFactory.maybeUpdateRolloverTime();
    RecordsWriter writer3 = datePartitionedRecordsWriterFactory.createWriter();

    // Assert
    assertThat(writer1.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_1.avro");
    assertThat(writer2.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_2.avro");
    assertThat(writer2.getPath().getParent().getName()).isEqualTo("2022-12-25");
    assertThat(writer3.getPath().getName()).isEqualTo("dwhassessment_" + TEST_UUID + "_1.avro");
    assertThat(writer3.getPath().getParent().getName()).isEqualTo("2022-12-26");
  }

  @Test
  public void shouldRollover_returnsFalseForSameDate() throws Exception {
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(parseDateTime("2022-12-25T12:23:54.00Z")) // Constructor
        .thenReturn(parseDateTime("2022-12-25T23:00:00.00Z")); // First date check
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);

    // Act, Assert
    assertThat(datePartitionedRecordsWriterFactory.shouldRollover()).isFalse();
  }

  @Test
  public void shouldRollover_returnsTrueForNextDate() throws Exception {
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(parseDateTime("2022-12-25T12:23:54.00Z")) // Constructor
        .thenReturn(parseDateTime("2022-12-26T09:00:00.00Z")); // First date check
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);

    // Act, Assert
    assertThat(datePartitionedRecordsWriterFactory.shouldRollover()).isTrue();
  }

  @Test
  public void shouldRollover_returnsTrueForNextDateOnTheEdge() throws Exception {
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(parseDateTime("2022-12-25T23:59:59.00Z")) // Constructor
        .thenReturn(parseDateTime("2022-12-26T00:00:00.01Z")); // First date check
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);

    // Act, Assert
    assertThat(datePartitionedRecordsWriterFactory.shouldRollover()).isTrue();
  }

  @Test
  public void maybeUpdateRolloverTime_ignoresIfOnTheSameDate() throws Exception {
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(parseDateTime("2022-12-25T12:23:54.00Z")) // Constructor
        .thenReturn(parseDateTime("2022-12-25T15:00:00.00Z")); // First date check
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);
    String currentDirectory =
        datePartitionedRecordsWriterFactory.createWriter().getPath().getParent().getName();

    // Act
    boolean isUpdated = datePartitionedRecordsWriterFactory.maybeUpdateRolloverTime();

    // Assert
    assertThat(isUpdated).isFalse();
    assertThat(currentDirectory).isEqualTo("2022-12-25");
    assertThat(getCurrentWriterFolder(datePartitionedRecordsWriterFactory)).isEqualTo("2022-12-25");
  }

  @Test
  public void maybeUpdateRolloverTime_updatesWhenOnTheDifferentDate() throws Exception {
    Instant firstInstant = parseDateTime("2022-12-25T12:23:54.00Z");
    Instant currentInstant = parseDateTime("2022-12-26T15:00:00.00Z");
    Clock clockMock = mock(Clock.class);
    when(clockMock.instant())
        .thenReturn(firstInstant, firstInstant) // Constructor and first RecordsWriter
        .thenReturn(currentInstant, currentInstant); // First date check and consecutive calls
    DatePartitionedRecordsWriterFactory datePartitionedRecordsWriterFactory =
        createRecordsWriterFactory(clockMock);
    String currentDirectory =
        datePartitionedRecordsWriterFactory.createWriter().getPath().getParent().getName();

    // Act
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
        new Path(tmpFolder), conf, QUERY_EVENT_SCHEMA, clock, UUID.fromString(TEST_UUID));
  }

  private Instant parseDateTime(String dateTimeString) {
    return Instant.parse(dateTimeString);
  }

  private String getCurrentWriterFolder(DatePartitionedRecordsWriterFactory recordsWriterFactory)
      throws IOException {
    return recordsWriterFactory.createWriter().getPath().getParent().getName();
  }
}
