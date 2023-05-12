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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
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
public class RecordsWriterTest {
  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private HiveConf conf;
  private Path outputFilePath;

  private RecordsWriter recordsWriter;

  @Before
  public void setup() throws IOException {
    conf = new HiveConf();
    outputFilePath = new Path(folder.newFolder().getAbsolutePath(), "temp_file.avro");
    recordsWriter = new RecordsWriter(conf, outputFilePath, QUERY_EVENT_SCHEMA);
  }

  @Test
  public void writeMessage_singleMessageSuccess() throws Exception {
    GenericRecord message = createMessage("test_id_1");

    // Act
    recordsWriter.writeMessage(message);
    recordsWriter.close();

    // Assert
    List<GenericRecord> records = readOutputRecords(conf, outputFilePath);
    assertThat(records).containsExactly(message);
  }

  @Test
  public void writeMessage_multipleMessageSuccess() throws Exception {
    GenericRecord message1 = createMessage("test_id_1");
    GenericRecord message2 = createMessage("test_id_2");

    // Act
    recordsWriter.writeMessage(message1);
    recordsWriter.writeMessage(message2);
    recordsWriter.close();

    // Assert
    List<GenericRecord> records = readOutputRecords(conf, outputFilePath);
    assertThat(records).containsExactly(message1, message2);
  }

  @Test
  public void getPath_success() {
    assertThat(recordsWriter.getPath()).isEqualTo(outputFilePath);
  }

  private static List<GenericRecord> readOutputRecords(HiveConf conf, Path outputFilePath)
      throws IOException {
    FileSystem fs = outputFilePath.getFileSystem(conf);
    FSDataInputStream inputStream = fs.open(outputFilePath);

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(QUERY_EVENT_SCHEMA);

    try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream, reader)) {
      List<GenericRecord> records = new ArrayList<>();
      dataFileReader.forEach(records::add);
      return records;
    }
  }

  private GenericRecord createMessage(String id) {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA).set("QueryId", id).build();
  }
}
