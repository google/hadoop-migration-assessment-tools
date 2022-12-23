package com.google.cloud.bigquery.dwhassessment.hooks.logger;


import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth8.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
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
public class EventRecordConstructorTest {

  @Rule
  public MockitoRule mocks = MockitoJUnit.rule();

  @Mock
  Hive hiveMock;

  private QueryState queryState;
  private HiveConf conf;

  private EventRecordConstructor eventRecordConstructor;

  private static final long QUERY_END_TIME = 9999L;

  @Before
  public void setup() {
    conf = new HiveConf();
    queryState = new QueryState(conf);

    Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(QUERY_END_TIME), ZoneOffset.UTC);
    eventRecordConstructor = new EventRecordConstructor(fixedClock);
  }

  @Test
  public void preExecHook_success() throws Exception {
    String queryText = "SELECT * FROM employees";
    String queryId = "hive_query_id_999";
    QueryPlan queryPlan = createQueryPlan(queryText, queryId);
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.PRE_EXEC_HOOK);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record)
        .hasValue(
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
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record)
        .hasValue(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", queryId)
                .set("EventType", "QUERY_COMPLETED")
                .set("EndTime", QUERY_END_TIME)
                .set("RequestUser", "test_user")
                .set("UserName", System.getProperty("user.name"))
                .set("OperationId", "test_op_id")
                .set("Status", "SUCCESS")
                .set("PerfObject", "{}")
                .set("HookVersion", "1.0")
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
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record)
        .hasValue(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", queryId)
                .set("EventType", "QUERY_COMPLETED")
                .set("EndTime", QUERY_END_TIME)
                .set("RequestUser", "test_user")
                .set("UserName", System.getProperty("user.name"))
                .set("OperationId", "test_op_id")
                .set("Status", "FAIL")
                .set("PerfObject", "{}")
                .set("HookVersion", "1.0")
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
}
