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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
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
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.Counters;
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

  private SessionState state;

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();

    queryState = new QueryState(conf);
    queryPlan = TestUtils.createDefaultQueryPlan(hiveMock, queryState);
    hookContext = TestUtils.createDefaultHookContext(queryPlan, queryState);

    state = new SessionState(conf);
    state.setMapRedStats(new HashMap<>());
    SessionState.setCurrentSessionState(state);

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
      @FromDataPoints("PostHookTypes") HookType hookType) throws ParseException {
    hookContext.setHookType(hookType);

    String countersString = "{(org.apache.hadoop.mapreduce.FileSystemCounter)(File System Counters)[(FILE_BYTES_READ)(FILE: Number of bytes read)(0)][(FILE_BYTES_WRITTEN)(FILE: Number of bytes written)(418993)][(FILE_READ_OPS)(FILE: Number of read operations)(0)][(FILE_LARGE_READ_OPS)(FILE: Number of large read operations)(0)][(FILE_WRITE_OPS)(FILE: Number of write operations)(0)][(HDFS_BYTES_READ)(HDFS: Number of bytes read)(21617)][(HDFS_BYTES_WRITTEN)(HDFS: Number of bytes written)(4461)][(HDFS_READ_OPS)(HDFS: Number of read operations)(8)][(HDFS_LARGE_READ_OPS)(HDFS: Number of large read operations)(0)][(HDFS_WRITE_OPS)(HDFS: Number of write operations)(2)]}{(org.apache.hadoop.mapreduce.JobCounter)(Job Counters )[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(1)][(RACK_LOCAL_MAPS)(Rack-local map tasks)(1)][(SLOTS_MILLIS_MAPS)(Total time spent by all maps in occupied slots \\(ms\\))(9501)][(SLOTS_MILLIS_REDUCES)(Total time spent by all reduces in occupied slots \\(ms\\))(0)][(MILLIS_MAPS)(Total time spent by all map tasks \\(ms\\))(3167)][(VCORES_MILLIS_MAPS)(Total vcore-milliseconds taken by all map tasks)(3167)][(MB_MILLIS_MAPS)(Total megabyte-milliseconds taken by all map tasks)(9729024)]}{(org.apache.hadoop.mapreduce.TaskCounter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(15)][(MAP_OUTPUT_RECORDS)(Map output records)(0)][(SPLIT_RAW_BYTES)(Input split bytes)(270)][(SPILLED_RECORDS)(Spilled Records)(0)][(FAILED_SHUFFLE)(Failed Shuffles)(0)][(MERGED_MAP_OUTPUTS)(Merged Map outputs)(0)][(GC_TIME_MILLIS)(GC time elapsed \\(ms\\))(137)][(CPU_MILLISECONDS)(CPU time spent \\(ms\\))(3000)][(PHYSICAL_MEMORY_BYTES)(Physical memory \\(bytes\\) snapshot)(368377856)][(VIRTUAL_MEMORY_BYTES)(Virtual memory \\(bytes\\) snapshot)(4413640704)][(COMMITTED_HEAP_BYTES)(Total committed heap usage \\(bytes\\))(337641472)]}{(HIVE)(HIVE)[(CREATED_FILES)(CREATED_FILES)(1)][(DESERIALIZE_ERRORS)(DESERIALIZE_ERRORS)(0)][(FATAL_ERROR)(FATAL_ERROR)(0)][(RECORDS_IN)(RECORDS_IN)(14)][(RECORDS_OUT_0)(RECORDS_OUT_0)(10)]}{(org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter)(File Input Format Counters )[(BYTES_READ)(Bytes Read)(0)]}{(org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter)(File Output Format Counters )[(BYTES_WRITTEN)(Bytes Written)(0)]}";
    Counters expectedCounters = Counters.fromEscapedCompactString(countersString);

    MapRedStats stats = new MapRedStats(0, 0, 0L, true, "1");
    stats.setCounters(expectedCounters);

    state.getMapRedStats().put("Map Stage 1", stats);

    // Act
    Optional<GenericRecord> record = eventRecordConstructor.constructEvent(hookContext);
    String countersReceived = record.get().get("MapReduceCountersObject").toString();


    // Assert
    assertThat(Counters.fromEscapedCompactString(countersReceived)).isEqualTo(expectedCounters);
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
