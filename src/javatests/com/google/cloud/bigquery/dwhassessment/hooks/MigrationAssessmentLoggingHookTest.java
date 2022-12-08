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
package com.google.cloud.bigquery.dwhassessment.hooks;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.dwhassessment.hooks.avro.AvroSchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class MigrationAssessmentLoggingHookTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Mock PerfLogger perfLoggerMock;
  @Mock Hive hiveMock;

  private static final Schema QUERY_EVENTS_SCHEMA = AvroSchemaLoader.loadSchema("QueryEvents.avsc");

  private QueryState queryState;

  private MigrationAssessmentLoggingHook hook;

  @Before
  public void setup() {
    hook = new MigrationAssessmentLoggingHook();
    queryState = new QueryState(new HiveConf());
  }

  @Test
  public void call_success() throws Exception {
    String queryText = "SELECT * FROM employees";
    String queryId = "hive_query_id_999";
    QueryPlan queryPlan = createQueryPlan(queryText, queryId);
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.PRE_EXEC_HOOK);

    // Act
    hook.run(context);

    // Assert
    assertThat(hook.records)
        .containsExactly(
            new GenericRecordBuilder(QUERY_EVENTS_SCHEMA)
                .set("QueryId", queryId)
                .set("QueryText", queryText)
                .set("EventType", "PRE_EXEC_HOOK")
                .set("ExecutionMode", "NONE")
                .set("Timestamp", 1234L)
                .set("RequestUser", "test_user")
                .set("User", System.getProperty("user.name"))
                .build());
  }

  private HookContext createContext(QueryPlan queryPlan) throws Exception {
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
        perfLoggerMock);
  }

  private QueryPlan createQueryPlan(String queryText, String queryId) throws Exception {
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(queryState, hiveMock);

    return new QueryPlan(queryText, sem, 1234L, queryId, HiveOperation.QUERY, null);
  }
}
