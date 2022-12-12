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

import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Writes {@link GenericRecord} messages to a given file. */
public class RecordsWriter implements Closeable {
  private final Path filePath;
  private final DataFileWriter<GenericRecord> dataFileWriter;

  RecordsWriter(Configuration conf, Path filePath, Schema schema) throws IOException {
    this.filePath = filePath;
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    dataFileWriter = new DataFileWriter<>(datumWriter);

    FileSystem fs = filePath.getFileSystem(conf);

    FSDataOutputStream outputStream = fs.create(filePath, true);
    dataFileWriter.create(schema, outputStream);
  }

  public Path getPath() {
    return filePath;
  }

  public void writeMessage(GenericRecord message) throws IOException {
    dataFileWriter.append(message);
  }

  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  public void close() throws IOException {
    dataFileWriter.close();
  }
}
