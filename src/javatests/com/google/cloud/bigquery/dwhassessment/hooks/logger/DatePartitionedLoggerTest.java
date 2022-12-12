package com.google.cloud.bigquery.dwhassessment.hooks.logger;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
public class DatePartitionedLoggerTest {
  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

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
    new DatePartitionedLogger(targetDirectoryPath, conf, QUERY_EVENT_SCHEMA, fixedClock);

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
    DatePartitionedLogger datePartitionedLogger =
        new DatePartitionedLogger(
            new Path(tmpFolder),
            conf,
            QUERY_EVENT_SCHEMA,
            Clock.fixed(fixedInstant, ZoneOffset.UTC));

    // Act
    RecordsWriter writer = datePartitionedLogger.createWriter("test_filename.avro");

    // Assert
    assertThat(writer.getPath())
        .isEqualTo(
            new Path(
                targetDirectoryPath.getFileSystem(conf).resolvePath(targetDirectoryPath),
                "test_filename.avro"));
    assertThat(fs.exists(targetDirectoryPath)).isTrue();
    assertThat(existedBefore).isFalse();
  }

  @Test
  public void getDateFromDir_success() {
    LocalDate expected = LocalDate.of(2022, 12, 8);

    assertThat(DatePartitionedLogger.getDateFromDir("2022-12-08")).isEqualTo(expected);
  }

  @Test
  public void getDateFromDir_invalidDirectoryName_fail() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> DatePartitionedLogger.getDateFromDir("test"));

    assertThat(e).hasMessageThat().isEqualTo("Invalid directory: test");
  }
}
