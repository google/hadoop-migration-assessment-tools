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
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
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
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class EventLoggerTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Mock Hive hiveMock;

  private static final long QUERY_COMPLETED_TIME = 100005L;

  private QueryState queryState;
  private HiveConf conf;
  private String tmpFolder;

  private EventLogger logger;

  @Before
  public void setup() throws IOException {
    conf = new HiveConf();
    tmpFolder = folder.newFolder().getAbsolutePath();
    conf.set(LoggerVarsConfig.HIVE_QUERY_EVENTS_BASE_PATH.getConfName(), tmpFolder);
    logger =
        new EventLogger(
            conf, Clock.fixed(Instant.ofEpochMilli(QUERY_COMPLETED_TIME), ZoneOffset.UTC));
    queryState = new QueryState(conf);
  }

  @Test
  public void preExecHook_success() throws Exception {
    String queryText = "SELECT * FROM employees";
    String queryId = "hive_query_id_999";
    QueryPlan queryPlan = createQueryPlan(queryText, queryId);
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.PRE_EXEC_HOOK);

    // Act
    logger.handle(context);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = readOutputRecords(conf, tmpFolder);
    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", queryId)
                .set("QueryText", queryText)
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
                .build());
  }

  @Test
  public void postExecHook_success() throws Exception {
    String queryText = "SELECT * FROM employees";
    String queryId = "hive_query_id_999";
    QueryPlan queryPlan = createQueryPlan(queryText, queryId);
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.POST_EXEC_HOOK);

    // Act
    logger.handle(context);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = readOutputRecords(conf, tmpFolder);
    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", queryId)
                .set("EventType", "QUERY_COMPLETED")
                .set("EndTime", QUERY_COMPLETED_TIME)
                .set("RequestUser", "test_user")
                .set("UserName", System.getProperty("user.name"))
                .set("OperationId", "test_op_id")
                .set("Status", "SUCCESS")
                .set("PerfObject", "{}")
                .set("HookVersion", "1.0")
                .set("MapReduceCountersObject", "[]")
                .set("TezCountersObject", "[]")
                .build());
  }

  @Test
  public void onFailureHook_success() throws Exception {
    String queryText = "SELECT * FROM employees";
    String queryId = "hive_query_id_999";
    QueryPlan queryPlan = createQueryPlan(queryText, queryId);
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.ON_FAILURE_HOOK);

    // Act
    logger.handle(context);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = readOutputRecords(conf, tmpFolder);
    assertThat(records)
        .containsExactly(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", queryId)
                .set("EventType", "QUERY_COMPLETED")
                .set("EndTime", QUERY_COMPLETED_TIME)
                .set("RequestUser", "test_user")
                .set("UserName", System.getProperty("user.name"))
                .set("OperationId", "test_op_id")
                .set("Status", "FAIL")
                .set("PerfObject", "{}")
                .set("HookVersion", "1.0")
                .set("MapReduceCountersObject", "[]")
                .set("TezCountersObject", "[]")
                .build());
  }

  private HookContext createContext(QueryPlan queryPlan) throws Exception {
    PerfLogger perfLogger = PerfLogger.getPerfLogger(conf, true);
    return new HookContext(
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

  private QueryPlan createQueryPlan(String queryText, String queryId) throws Exception {
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(queryState, hiveMock);

    return new QueryPlan(queryText, sem, 1234L, queryId, HiveOperation.QUERY, null);
  }

  private static List<GenericRecord> readOutputRecords(HiveConf conf, String tmpFolder)
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
      List<GenericRecord> records = new ArrayList<>();
      dataFileReader.forEach(records::add);
      return records;
    }
  }
}
