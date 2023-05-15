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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth8.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.dwhassessment.hooks.testing.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.CopyTask;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
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

  private EventRecordConstructor eventRecordConstructor;

  private HookContext hookContext;
  private QueryState queryState;
  private QueryPlan queryPlan;
  private SessionState state;

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();

    queryState = new QueryState(conf);
    state = TestUtils.createDefaultSessionState(conf);
    queryPlan = TestUtils.createDefaultQueryPlan(hiveMock, queryState);
    hookContext = TestUtils.createDefaultHookContext(queryPlan, queryState);

    eventRecordConstructor = new EventRecordConstructor(TestUtils.createFixedClock());
  }

  @Test
  public void preExecHook_success() {
    hookContext.setHookType(HookType.PRE_EXEC_HOOK);
    queryState.setCommandType(HiveOperation.QUERY);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record).hasValue(TestUtils.createPreExecRecordBuilder().build());
  }

  @DataPoints("ExecutionModes")
  public static final ImmutableList<ExecutionModeTestCase> EXECUTION_MODE_TEST_CASES =
      ImmutableList.of(
          ExecutionModeTestCase.create(
              "TEZ", createTezTaskWithNullCounters("id1", /* isLlapMode= */ false)),
          ExecutionModeTestCase.create(
              "LLAP",
              createTezTaskWithNullCounters("id1", /* isLlapMode= */ false),
              createTezTaskWithNullCounters("id2", /* isLlapMode= */ true)),
          ExecutionModeTestCase.create("MR", new ExecDriver()),
          ExecutionModeTestCase.create("DDL", new DDLTask()),
          ExecutionModeTestCase.create("NONE", new CopyTask()));

  @Theory
  public void preExecHook_executionMode(
      @FromDataPoints("ExecutionModes") ExecutionModeTestCase testCase) {
    hookContext.setHookType(HookType.PRE_EXEC_HOOK);
    queryState.setCommandType(HiveOperation.QUERY);
    queryPlan.setRootTasks(new ArrayList<>(testCase.tasks()));

    // Act
    GenericRecord record = eventRecordConstructor.constructEvent(hookContext).get();

    // Assert
    assertThat(record.get("ExecutionMode")).isEqualTo(testCase.executionMode());
  }

  @Test
  public void preExecHook_shouldGetDatabase() {
    hookContext.setHookType(HookType.PRE_EXEC_HOOK);
    queryState.setCommandType(HiveOperation.QUERY);
    SessionState.get().setCurrentDatabase("DB");

    // Act
    GenericRecord record = eventRecordConstructor.constructEvent(hookContext).get();

    // Assert
    assertThat(record.get("DefaultDatabase")).isEqualTo("DB");
  }

  @Test
  public void postExecHook_success() {
    hookContext.setHookType(HookType.POST_EXEC_HOOK);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record).hasValue(TestUtils.createPostExecRecord(EventStatus.SUCCESS));
  }

  @Test
  public void onFailureHook_success() {
    hookContext.setHookType(HookType.ON_FAILURE_HOOK);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record).hasValue(TestUtils.createPostExecRecord(EventStatus.FAIL));
  }

  @DataPoints("PostHookTypes")
  public static final ImmutableList<HookType> POST_HOOK_TYPES =
      ImmutableList.of(HookType.ON_FAILURE_HOOK, HookType.POST_EXEC_HOOK);

  @Theory
  public void postHooks_shouldStoreMapReduceCounters(
      @FromDataPoints("PostHookTypes") HookType hookType) {
    hookContext.setHookType(hookType);

    CountersHolder countersHolder =
        CountersHolder.builder()
            .addGroup(
                CountersGroupHolder.builder()
                    .setName("counters_group1")
                    .setCounters(ImmutableMap.of("metric_key1", 123L))
                    .build())
            .addGroup(
                CountersGroupHolder.builder()
                    .setName("counters_group2")
                    .setCounters(
                        ImmutableMap.of(
                            "metric_key1", 456L,
                            "metric_key2", 789L))
                    .build())
            .build();

    Counters expectedCounters = createMapReduceCounters(countersHolder);

    MapRedStats stats =
        new MapRedStats(
            /* numMap= */ 0,
            /* numReduce= */ 0,
            /* cpuMSec= */ 0L,
            /* ifSuccess= */ true,
            /* jobId= */ "1");
    stats.setCounters(expectedCounters);

    state.getMapRedStats().put("Map Stage 1", stats);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record.get().get("CountersObject"))
        .isEqualTo(
            "[[{\"counters_group1\":{\"metric_key1\":123}},{\"counters_group2\":{\"metric_key1\":456,\"metric_key2\":789}}]]");
  }

  @Theory
  public void postHooks_shouldStoreTezTasksCounters(
      @FromDataPoints("PostHookTypes") HookType hookType) {
    hookContext.setHookType(hookType);

    ImmutableList<Task<? extends Serializable>> tezTasks =
        ImmutableList.of(
            createTezTaskWithNullCounters("id1", /* isLlapMode= */ false),
            createTezTaskWithCounters(
                "id2",
                CountersHolder.builder()
                    .addGroup(
                        CountersGroupHolder.builder()
                            .setName("counters_group1")
                            .setCounters(ImmutableMap.of("task_key1", 123L))
                            .build())
                    .addGroup(
                        CountersGroupHolder.builder()
                            .setName("counters_group2")
                            .setCounters(
                                ImmutableMap.of(
                                    "task_key1", 456L,
                                    "task_key2", 789L))
                            .build())
                    .build()));
    queryPlan.setRootTasks(new ArrayList<>(tezTasks));

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record.get().get("CountersObject"))
        .isEqualTo(
            "[[{\"counters_group1\":{\"task_key1\":123}},{\"counters_group2\":{\"task_key1\":456,\"task_key2\":789}}]]");
  }

  private TezTask createTezTaskWithCounters(String id, CountersHolder counters) {
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

  private Counters createMapReduceCounters(CountersHolder countersHolder) {
    Counters counters = new Counters();

    countersHolder
        .groups()
        .forEach(
            group -> {
              Group countersGroup = counters.getGroup(group.name());
              group
                  .counters()
                  .forEach((key, value) -> countersGroup.getCounterForName(key).setValue(value));
            });

    return counters;
  }

  private static TezTask createTezTaskWithNullCounters(String id, boolean isLlapMode) {
    TezWork tezWork = mock(TezWork.class);
    when(tezWork.getLlapMode()).thenReturn(isLlapMode);

    TezTask task = mock(TezTask.class);
    when(task.getId()).thenReturn(id);
    when(task.getWork()).thenReturn(tezWork);
    when(task.getTezCounters()).thenReturn(null);

    return task;
  }

  @AutoValue
  abstract static class ExecutionModeTestCase {
    abstract String executionMode();

    abstract ImmutableList<Task<? extends Serializable>> tasks();

    static ExecutionModeTestCase create(
        String executionMode, Task<? extends Serializable>... tasks) {
      return new AutoValue_EventRecordConstructorTest_ExecutionModeTestCase(
          executionMode, ImmutableList.copyOf(tasks));
    }
  }

  /** Component that simplifies {@link Counters} and {@link TezCounters} setup. */
  @AutoValue
  abstract static class CountersHolder {
    abstract ImmutableList<CountersGroupHolder> groups();

    public static Builder builder() {
      return new AutoValue_EventRecordConstructorTest_CountersHolder.Builder();
    }

    /** Builder for {@link CountersHolder} */
    @AutoValue.Builder
    abstract static class Builder {
      abstract ImmutableList.Builder<CountersGroupHolder> groupsBuilder();

      public final Builder addGroup(CountersGroupHolder value) {
        groupsBuilder().add(value);
        return this;
      }

      public abstract CountersHolder build();
    }
  }

  /** Component that simplifies {@link Counters.Group} and {@link CounterGroup} setup. */
  @AutoValue
  abstract static class CountersGroupHolder {

    abstract String name();

    abstract ImmutableMap<String, Long> counters();

    public static Builder builder() {
      return new AutoValue_EventRecordConstructorTest_CountersGroupHolder.Builder();
    }

    /** Builder for {@link CountersGroupHolder} */
    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder setName(String value);

      public abstract Builder setCounters(ImmutableMap<String, Long> value);

      public abstract CountersGroupHolder build();
    }
  }
}
