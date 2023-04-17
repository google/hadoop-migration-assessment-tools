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

package com.google.cloud.bigquery.dwhassessment.hooks.test_utils;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/** Common utils for testing */
public final class TestUtils {

  public static final long QUERY_END_TIME = 9999L;
  public static final String DEFAULT_QUERY_TEXT = "SELECT * FROM employees";
  public static final String DEFAULT_QUERY_ID = "hive_query_id_999";

  private final HiveConf conf;
  private final QueryState queryState;
  private final QueryPlan queryPlan;
  private final HookContext hookContext;

  public TestUtils(Hive hiveMock) throws Exception {
    conf = new HiveConf();
    queryState = new QueryState(conf);
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(queryState, hiveMock);
    queryPlan = new QueryPlan(DEFAULT_QUERY_TEXT, sem, 1234L, DEFAULT_QUERY_ID,
        HiveOperation.QUERY, null);
    PerfLogger perfLogger = PerfLogger.getPerfLogger(conf, true);
    hookContext = new HookContext(
        queryPlan,
        queryState,
        null,
        "test_user",
        "192.168.10.10",
        "hive_addr",
        "test_op_id",
        "test_session_id",
        "test_thread_id",
        true,
        perfLogger);
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public HookContext getHookContext() {
    return hookContext;
  }

  public HiveConf getConf() {
    return conf;
  }

  public static Clock getFixedClock() {
    return Clock.fixed(Instant.ofEpochMilli(QUERY_END_TIME), ZoneOffset.UTC);
  }

  public static Record getPreExecRecord() {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", TestUtils.DEFAULT_QUERY_ID)
        .set("QueryText", TestUtils.DEFAULT_QUERY_TEXT)
        .set("EventType", "QUERY_SUBMITTED")
        .set("ExecutionMode", "NONE")
        .set("StartTime", 1234L)
        .set("RequestUser", "test_user")
        .set("UserName", System.getProperty("user.name"))
        .set("SessionId", "test_session_id")
        .set("IsTez", false)
        .set("IsMapReduce", false)
        .set("InvokerInfo", "test_session_id")
        .set("ThreadName", "test_thread_id")
        .set("HookVersion", "1.0")
        .set("ClientIpAddress", "192.168.10.10")
        .set("HiveAddress", "hive_addr")
        .set("HiveInstanceType", "HS2")
        .set("OperationId", "test_op_id")
        .set("MapReduceCountersObject", "[]")
        .set("TezCountersObject", "[]")
        .build();
  }

  public static Record getPostExecRecord(String status) {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", TestUtils.DEFAULT_QUERY_ID)
        .set("EventType", "QUERY_COMPLETED")
        .set("EndTime", TestUtils.QUERY_END_TIME)
        .set("RequestUser", "test_user")
        .set("UserName", System.getProperty("user.name"))
        .set("OperationId", "test_op_id")
        .set("Status", status)
        .set("PerfObject", "{}")
        .set("HookVersion", "1.0")
        .set("MapReduceCountersObject", "[]")
        .set("TezCountersObject", "[]")
        .build();
  }

  public static List<GenericRecord> readOutputRecords(HiveConf conf, String tmpFolder)
      throws IOException {
    Path path = new Path(tmpFolder);
    FileSystem fs = path.getFileSystem(conf);

    ImmutableList<FileStatus> directories = ImmutableList.copyOf(fs.listStatus(path));
    assertThat(directories).hasSize(1);
    ImmutableList<FileStatus> files =  ImmutableList.copyOf(fs.listStatus(directories.get(0).getPath()));
    assertThat(files).hasSize(1);

    FSDataInputStream inputStream = fs.open(files.get(0).getPath());

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(QUERY_EVENT_SCHEMA);

    try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream, reader)) {
      ArrayList<GenericRecord> records = new ArrayList<>();
      dataFileReader.forEach(records::add);
      return records;
    }
  }
}
