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

package com.google.cloud.bigquery.dwhassessment.hooks.testing;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.EventStatus;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

/** Common utils for testing */
public final class TestUtils {

  public static final long QUERY_END_TIME = 9999L;
  public static final String DEFAULT_QUERY_TEXT = "SELECT * FROM employees";
  public static final String DEFAULT_QUERY_ID = "hive_query_id_999";

  private TestUtils() {}

  public static SessionState createDefaultSessionState(HiveConf conf) {
    SessionState state = new SessionState(conf);
    state.setMapRedStats(new HashMap<>());
    SessionState.setCurrentSessionState(state);
    return state;
  }

  public static QueryPlan createDefaultQueryPlan(Hive hive, QueryState state)
      throws SemanticException {
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(state, hive);
    return new QueryPlan(
        DEFAULT_QUERY_TEXT, sem, 1234L, DEFAULT_QUERY_ID, HiveOperation.QUERY, null);
  }

  public static QueryPlan createQueryPlanWithPartitions(
      Hive hive,
      QueryState state,
      ImmutableList<org.apache.hadoop.hive.metastore.api.Partition> mockedPartitions,
      org.apache.hadoop.hive.metastore.api.Table mockTable)
      throws Exception {
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(state, hive);
    QueryPlan qp =
        new QueryPlan(DEFAULT_QUERY_TEXT, sem, 1234L, DEFAULT_QUERY_ID, HiveOperation.QUERY, null);

    Table table = new Table(mockTable);
    table.setTableType(TableType.MANAGED_TABLE);
    HashSet<ReadEntity> readEntities = new HashSet<>();
    HashSet<WriteEntity> writeEntities = new HashSet<>();
    for (org.apache.hadoop.hive.metastore.api.Partition mockedPartition : mockedPartitions) {
      Partition partition = new Partition(table, mockedPartition);
      ReadEntity readEntity = new ReadEntity(partition);
      readEntities.add(readEntity);
      WriteEntity writeEntity = new WriteEntity(partition, WriteType.INSERT);
      writeEntities.add(writeEntity);
    }
    qp.setInputs(readEntities);
    qp.setOutputs(writeEntities);
    return qp;
  }

  public static HookContext createDefaultHookContext(Hive hive, QueryState state) throws Exception {
    QueryPlan plan = createDefaultQueryPlan(hive, state);
    return createDefaultHookContext(plan, state);
  }

  public static HookContext createDefaultHookContext(QueryPlan plan, QueryState state)
      throws Exception {
    PerfLogger perfLogger = PerfLogger.getPerfLogger(state.getConf(), true);
    return new HookContext(
        plan,
        state,
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

  public static Clock createFixedClock() {
    return Clock.fixed(Instant.ofEpochMilli(QUERY_END_TIME), ZoneOffset.UTC);
  }

  public static GenericRecordBuilder createPreExecRecordBuilder() {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", TestUtils.DEFAULT_QUERY_ID)
        .set("QueryType", "QUERY")
        .set("QueryText", TestUtils.DEFAULT_QUERY_TEXT)
        .set("EventType", "QUERY_SUBMITTED")
        .set("ExecutionMode", "CLIENT_ONLY")
        .set("ExecutionEngine", "mr")
        .set("StartTime", 1234L)
        .set("RequestUser", "test_user")
        .set("UserName", System.getProperty("user.name"))
        .set("SessionId", "test_session_id")
        .set("InvokerInfo", "test_session_id")
        .set("ThreadName", "test_thread_id")
        .set("HookVersion", "1.0")
        .set("HiveVersion", "2.2.0")
        .set("ClientIpAddress", "192.168.10.10")
        .set("HiveAddress", "hive_addr")
        .set("HiveInstanceType", "HS2")
        .set("OperationId", "test_op_id")
        .set("DefaultDatabase", "default");
  }

  public static Record createPostExecRecord(EventStatus status) {
    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", TestUtils.DEFAULT_QUERY_ID)
        .set("EventType", "QUERY_COMPLETED")
        .set("EndTime", TestUtils.QUERY_END_TIME)
        .set("RequestUser", "test_user")
        .set("UserName", System.getProperty("user.name"))
        .set("OperationId", "test_op_id")
        .set("Status", status.name())
        .set("PerfObject", "{}")
        .set("HookVersion", "1.0")
        .build();
  }

  public static List<GenericRecord> readOutputRecords(HiveConf conf, String tmpFolder)
      throws IOException {
    Path path = new Path(tmpFolder);
    FileSystem fs = path.getFileSystem(conf);

    ImmutableList<FileStatus> directories = ImmutableList.copyOf(fs.listStatus(path));
    assertThat(directories).hasSize(1);
    ImmutableList<FileStatus> files =
        ImmutableList.copyOf(fs.listStatus(directories.get(0).getPath()));
    assertThat(files).hasSize(1);

    FSDataInputStream inputStream = fs.open(files.get(0).getPath());

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(QUERY_EVENT_SCHEMA);

    try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(inputStream, reader)) {
      ArrayList<GenericRecord> records = new ArrayList<>();
      dataFileReader.forEach(records::add);
      return records;
    }
  }

  public static MapRedStats createMapRedStats(String jobId) {
    return new MapRedStats(
        /* numMap= */ 0,
        /* numReduce= */ 0,
        /* cpuMSec= */ 0L,
        /* ifSuccess= */ true,
        jobId);
  }
}
