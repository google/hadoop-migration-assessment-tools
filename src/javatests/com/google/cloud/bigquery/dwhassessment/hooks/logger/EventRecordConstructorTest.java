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
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.metadata.Hive;
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

  @Rule
  public MockitoRule mocks = MockitoJUnit.rule();

  @Mock
  Hive hiveMock;

  private EventRecordConstructor eventRecordConstructor;

  private HookContext hookContext;
  private QueryState queryState;
  private QueryPlan queryPlan;

  @Before
  public void setup() throws Exception {
    queryState = TestUtils.createDefaultQueryState();
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
    assertThat(record).hasValue(TestUtils.createPreExecRecord());
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
  public void postHooks_shouldStoreMapReduceTasksCounters(
      @FromDataPoints("PostHookTypes") HookType hookType) {
    hookContext.setHookType(hookType);

    ImmutableList<Task<? extends Serializable>> mrTasks =
        ImmutableList.of(
            createMapReduceTaskWithCounters(
                "id1", ImmutableMap.of("task_key1", 123L, "task_key2", 456L)),
            createMapReduceTaskWithCounters("id2", ImmutableMap.of("task_key1", 999L)));
    queryPlan.setRootTasks(new ArrayList<>(mrTasks));

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record.get().get("MapReduceCountersObject"))
        .isEqualTo("[{\"task_key1\":123,\"task_key2\":456},{\"task_key1\":999}]");
  }

  @Theory
  public void postHooks_shouldStoreTezTasksCounters(
      @FromDataPoints("PostHookTypes") HookType hookType) {
    hookContext.setHookType(hookType);

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
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);

    // Assert
    assertThat(record.get().get("TezCountersObject"))
        .isEqualTo(
            "[[{\"counters_group1\":{\"task_key1\":123}},{\"counters_group2\":{\"task_key1\":456,\"task_key2\":789}}]]");
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

}
