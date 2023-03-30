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
import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Theories.class)
public class EventRecordConstructorTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Mock Hive hiveMock;

  private QueryState queryState;
  private HiveConf conf;

  private EventRecordConstructor eventRecordConstructor;

  private static final long QUERY_END_TIME = 9999L;
  private static final String DEFAULT_QUERY_TEXT = "SELECT * FROM employees";
  private static final String DEFAULT_QUERY_ID = "hive_query_id_999";

  @Before
  public void setup() {
    conf = new HiveConf();
    queryState = new QueryState(conf);

    Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(QUERY_END_TIME), ZoneOffset.UTC);
    eventRecordConstructor = new EventRecordConstructor(fixedClock);
  }

  @Test
  public void preExecHook_success() throws Exception {
    QueryPlan queryPlan = createQueryPlan();
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.PRE_EXEC_HOOK);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record)
        .hasValue(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", DEFAULT_QUERY_ID)
                .set("QueryText", DEFAULT_QUERY_TEXT)
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
    QueryPlan queryPlan = createQueryPlan();
    HookContext context = createContext(queryPlan);
    context.setHookType(HookType.POST_EXEC_HOOK);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record)
        .hasValue(
            new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
                .set("QueryId", DEFAULT_QUERY_ID)
                .set("EventType", "QUERY_COMPLETED")
                .set("EndTime", QUERY_END_TIME)
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
                .set("MapReduceCountersObject", "[]")
                .set("TezCountersObject", "[]")
                .build());
  }

  @DataPoints("AllHookTypes")
  public static final ImmutableList<HookType> ALL_HOOK_TYPES =
      ImmutableList.of(HookType.PRE_EXEC_HOOK, HookType.ON_FAILURE_HOOK, HookType.POST_EXEC_HOOK);

  @Theory
  public void allHooks_shouldStoreMapReduceTasksCounters(
      @FromDataPoints("AllHookTypes") HookType hookType) throws Exception {
    QueryPlan queryPlan = createQueryPlan();
    HookContext context = createContext(queryPlan);
    context.setHookType(hookType);

    ImmutableList<Task<? extends Serializable>> mrTasks =
        ImmutableList.of(
            createMapReduceTaskWithCounters(
                "id1", ImmutableMap.of("task_key1", 123L, "task_key2", 456L)),
            createMapReduceTaskWithCounters("id2", ImmutableMap.of("task_key1", 999L)));
    queryPlan.setRootTasks(new ArrayList<>(mrTasks));

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record.get().get("MapReduceCountersObject"))
        .isEqualTo("[{\"task_key1\":123,\"task_key2\":456},{\"task_key1\":999}]");
  }

  @Theory
  public void allHooks_shouldStoreTezTasksCounters(
      @FromDataPoints("AllHookTypes") HookType hookType) throws Exception {
    QueryPlan queryPlan = createQueryPlan();
    HookContext context = createContext(queryPlan);
    context.setHookType(hookType);

    ImmutableList<Task<? extends Serializable>> tezTasks =
        ImmutableList.of(
            createTezTaskWithNullCounters("id1"),
            createTezTaskWithCounters(
                "id2",
                TezTaskCounterHolder.builder()
                    .addGroup(
                        TezTaskCounterGroupHolder.builder()
                            .setName("counters_group1")
                            .setCounters(ImmutableMap.of("task_key1", 123L))
                            .build())
                    .addGroup(
                        TezTaskCounterGroupHolder.builder()
                            .setName("counters_group2")
                            .setCounters(
                                ImmutableMap.of(
                                    "task_key1", 456L,
                                    "task_key2", 789L))
                            .build())
                    .build()));
    queryPlan.setRootTasks(new ArrayList<>(tezTasks));

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(context);

    // Assert
    assertThat(record.get().get("TezCountersObject"))
        .isEqualTo(
            "[[{\"counters_group1\":{\"task_key1\":123}},{\"counters_group2\":{\"task_key1\":456,\"task_key2\":789}}]]");
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

  private QueryPlan createQueryPlan() throws Exception {
    return createQueryPlan(DEFAULT_QUERY_TEXT, DEFAULT_QUERY_ID);
  }

  private ExecDriver createMapReduceTaskWithCounters(
      String id, ImmutableMap<String, Long> counters) {
    ExecDriver task = new ExecDriver();
    task.setId(id);
    task.taskCounters = new HashMap<>(counters);
    return task;
  }

  private TezTask createTezTaskWithCounters(String id, TezTaskCounterHolder counters) {
    TezWork tezWork = mock(TezWork.class);
    when(tezWork.getLlapMode()).thenReturn(false);

    TezCounters tezCounters = new TezCounters();
    counters
        .groups()
        .forEach(
            groupHolder -> {
              CounterGroup group = tezCounters.addGroup(groupHolder.name(), groupHolder.name());
              groupHolder
                  .counters()
                  .forEach(
                      (counterKey, counterValue) ->
                          group.addCounter(counterKey, counterKey, counterValue));
            });

    TezTask task = mock(TezTask.class);
    when(task.getId()).thenReturn(id);
    when(task.getWork()).thenReturn(tezWork);
    when(task.getTezCounters()).thenReturn(tezCounters);

    return task;
  }

  private TezTask createTezTaskWithNullCounters(String id) {
    TezWork tezWork = mock(TezWork.class);
    when(tezWork.getLlapMode()).thenReturn(false);

    TezTask task = mock(TezTask.class);
    when(task.getId()).thenReturn(id);
    when(task.getWork()).thenReturn(tezWork);
    when(task.getTezCounters()).thenReturn(null);

    return task;
  }

  /** Component that simplifies {@link TezCounters} setup. */
  @AutoValue
  abstract static class TezTaskCounterHolder {
    abstract ImmutableList<TezTaskCounterGroupHolder> groups();

    public static Builder builder() {
      return new AutoValue_EventRecordConstructorTest_TezTaskCounterHolder.Builder();
    }

    /** Builder for {@link TezTaskCounterHolder} */
    @AutoValue.Builder
    abstract static class Builder {
      abstract ImmutableList.Builder<TezTaskCounterGroupHolder> groupsBuilder();

      public final Builder addGroup(TezTaskCounterGroupHolder value) {
        groupsBuilder().add(value);
        return this;
      }

      public abstract TezTaskCounterHolder build();
    }
  }

  /** Component that simplifies {@link CounterGroup} setup. */
  @AutoValue
  abstract static class TezTaskCounterGroupHolder {

    abstract String name();

    abstract ImmutableMap<String, Long> counters();

    public static Builder builder() {
      return new AutoValue_EventRecordConstructorTest_TezTaskCounterGroupHolder.Builder();
    }

    /** Builder for {@link TezTaskCounterGroupHolder} */
    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder setName(String value);

      public abstract Builder setCounters(ImmutableMap<String, Long> value);

      public abstract TezTaskCounterGroupHolder build();
    }
  }

  private QueryPlan createQueryPlan(String queryText, String queryId) throws Exception {
    BaseSemanticAnalyzer sem = new DDLSemanticAnalyzer(queryState, hiveMock);

    return new QueryPlan(queryText, sem, 1234L, queryId, HiveOperation.QUERY, null);
  }
}
