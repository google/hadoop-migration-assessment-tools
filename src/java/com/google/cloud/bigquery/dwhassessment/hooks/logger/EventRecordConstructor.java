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

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.MR_QUEUE_NAME;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig.TEZ_QUEUE_NAME;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.HOOK_VERSION;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.LOGGER_NAME;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidator.getHiveVersion;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.DATABASE;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.PARTITION;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.ReflectionMethods;
import com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.TasksRetriever;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Constructor for generic records for given hook event. */
public class EventRecordConstructor {

  private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);

  private final Clock clock;
  private final YarnApplicationRetriever yarnApplicationRetriever;

  public EventRecordConstructor(Clock clock, YarnApplicationRetriever yarnApplicationRetriever) {
    this.clock = clock;
    this.yarnApplicationRetriever = yarnApplicationRetriever;
  }

  /** Constructs a record with information specific to a hook type */
  public Optional<GenericRecord> constructEvent(HookContext hookContext) {
    switch (hookContext.getHookType()) {
      case PRE_EXEC_HOOK:
        return Optional.of(getPreHookEvent(hookContext));
      case POST_EXEC_HOOK:
        return Optional.of(getPostHookEvent(hookContext, EventStatus.SUCCESS));
      case ON_FAILURE_HOOK:
        return Optional.of(getPostHookEvent(hookContext, EventStatus.FAIL));
    }

    LOG.warn("Ignoring event of type: '{}'", hookContext.getHookType());
    return Optional.empty();
  }

  private GenericRecord getPreHookEvent(HookContext hookContext) {
    QueryPlan plan = hookContext.getQueryPlan();

    // Make a copy so that we do not modify hookContext conf.
    HiveConf conf = new HiveConf(hookContext.getConf());
    ExecutionMode executionMode = getExecutionMode(plan);
    Set<ReadEntity> inputs = ReflectionMethods.INSTANCE.getInputs(plan);
    Set<WriteEntity> outputs = ReflectionMethods.INSTANCE.getOutputs(plan);

    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", plan.getQueryId())
        .set("QueryType", hookContext.getQueryState().getCommandType())
        .set("QueryText", plan.getQueryStr())
        .set("EventType", EventType.QUERY_SUBMITTED.name())
        .set("StartTime", plan.getQueryStartTime())
        .set("UserName", getUser(hookContext))
        .set("RequestUser", getRequestUser(hookContext))
        .set("ExecutionMode", executionMode.name())
        .set("ExecutionEngine", conf.get("hive.execution.engine"))
        .set("Queue", retrieveSessionQueueName(conf, executionMode))
        .set("TablesRead", getTablesFromEntitySet(inputs))
        .set("TablesWritten", getTablesFromEntitySet(outputs))
        .set("PartitionsRead", getPartitionsFromEntitySet(inputs))
        .set("PartitionsWritten", getPartitionsFromEntitySet(outputs))
        .set("SessionId", hookContext.getSessionId())
        .set("InvokerInfo", conf.getLogIdVar(hookContext.getSessionId()))
        .set("ThreadName", hookContext.getThreadId())
        .set("ClientIpAddress", hookContext.getIpAddress())
        .set("ClientIpAddress", hookContext.getIpAddress())
        .set("HookVersion", HOOK_VERSION)
        .set("HiveVersion", getHiveVersion())
        .set("HiveAddress", getHiveInstanceAddress(hookContext))
        .set("HiveInstanceType", getHiveInstanceType(hookContext))
        .set("OperationId", hookContext.getOperationId())
        .set("DatabasesRead", getDatabasesFromEntitySet(inputs))
        .set("DatabasesWritten", getDatabasesFromEntitySet(outputs))
        .set("DefaultDatabase", SessionState.get().getCurrentDatabase())
        .build();
  }

  private GenericRecord getPostHookEvent(HookContext hookContext, EventStatus status) {
    QueryPlan plan = hookContext.getQueryPlan();
    LOG.info("Received post-hook notification for: '{}'", plan.getQueryId());
    // Make a copy so that we do not modify hookContext conf.
    HiveConf conf = new HiveConf(hookContext.getConf());

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
            .set("QueryId", plan.getQueryId())
            .set("EventType", EventType.QUERY_COMPLETED.name())
            .set("EndTime", clock.millis())
            .set("UserName", getUser(hookContext))
            .set("RequestUser", getRequestUser(hookContext))
            .set("Status", status.name())
            .set("ErrorMessage", hookContext.getErrorMessage())
            .set("HookVersion", HOOK_VERSION)
            .set("PerfObject", dumpPerfData(hookContext.getPerfLogger()))
            .set("OperationId", hookContext.getOperationId());

    ApplicationIdRetriever.determineApplicationId(conf, getExecutionMode(plan))
        .ifPresent(
            applicationId -> {
              recordBuilder.set("YarnApplicationId", applicationId.toString());
              yarnApplicationRetriever
                  .retrieve(conf, applicationId)
                  .ifPresent(
                      applicationReport -> {
                        recordBuilder.set("HiveHostName", applicationReport.getHost());
                        recordBuilder.set("Queue", applicationReport.getQueue());
                      });
            });

    dumpTezCounters(plan)
        .map(Optional::of)
        .orElseGet(EventRecordConstructor::dumpMapReduceCounters)
        .ifPresent(counters -> recordBuilder.set("CountersObject", counters));

    return recordBuilder.build();
  }

  /**
   * Retrieves YARN queue name where query is supposed to land. It might be different in reality,
   * depending on YARN configuration.
   *
   * <p>In combination with the queue name in YARN Application data from {@link
   * YarnApplicationRetriever#retrieve(HiveConf, ApplicationId)}} it makes the best effort in
   * extracting the query queue name.
   */
  private static String retrieveSessionQueueName(HiveConf conf, ExecutionMode mode) {
    switch (mode) {
      case LLAP:
        return conf.get(HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.varname);
      case MR:
        return conf.get(MR_QUEUE_NAME.getConfName());
      case TEZ:
        return conf.get(TEZ_QUEUE_NAME.getConfName());
      default:
        return null;
    }
  }

  /**
   * Dumps MapReduce jobs counters. For one query, there can be multiple map reduce jobs.
   *
   * <p>Note: {@link org.apache.hadoop.mapred.Counters} is deprecated. If, for any reason, this
   * becomes an issue in the future, use {@link org.apache.hadoop.mapreduce.Counters}
   */
  private static Optional<String> dumpMapReduceCounters() {

    List<Counters> list =
        SessionState.get().getMapRedStats().values().stream()
            .map(MapRedStats::getCounters)
            .collect(Collectors.toList());
    return generateCountersJson(list, c -> c.getName(), c -> c.getValue(), Group::getDisplayName);
  }

  private static Optional<String> dumpTezCounters(QueryPlan plan) {
    List<Task<?>> rootTasks = ReflectionMethods.INSTANCE.getRootTasks(plan);
    List<TezTask> tezTasks = Utilities.getTezTasks(rootTasks);
    List<TezCounters> list =
        tezTasks.stream().map(TezTask::getTezCounters).collect(Collectors.toList());
    return generateCountersJson(
        list, TezCounter::getName, TezCounter::getValue, CounterGroup::getDisplayName);
  }

  /**
   * Counters are deeply nested sets of key - value pairs. This attempts to dump them as is,
   * preserving their original structure.
   */
  private static <C, G extends Iterable<C>> Optional<String> generateCountersJson(
      List<? extends Iterable<G>> list,
      Function<C, String> nameFn,
      Function<C, Long> valueFn,
      Function<G, String> displayNameFn) {
    JSONArray outerObj = new JSONArray();

    list.forEach(
        counters -> {
          if (counters == null) {
            return;
          }

          JSONArray innerObj = new JSONArray();
          counters.forEach(
              counterGroup -> {
                JSONObject groupCounters =
                    new JSONObject(
                        StreamSupport.stream(counterGroup.spliterator(), false)
                            .collect(Collectors.toMap(nameFn, valueFn)));
                JSONObject counterGroupData =
                    new JSONObject().put(displayNameFn.apply(counterGroup), groupCounters);

                innerObj.put(counterGroupData);
              });

          outerObj.put(innerObj);
        });

    return outerObj.length() > 0 ? Optional.of(outerObj.toString()) : Optional.empty();
  }

  private String dumpPerfData(PerfLogger perfLogger) {
    Map<String, Long> perfStats = ReflectionMethods.INSTANCE.getStartTimes(perfLogger);
    JSONObject perfObj = new JSONObject();
    long now = clock.millis();

    for (String key : perfStats.keySet()) {
      long duration = perfLogger.getDuration(key);
      // Some perf logger entries are finished after the hook. Make the best effort to capture them
      // here with the duration at the current time.
      if (duration == 0L) {
        duration = now - perfLogger.getStartTime(key);
      }
      perfObj.put(key, duration);
    }

    return perfObj.toString();
  }

  private static Set<String> getTablesFromEntitySet(Set<? extends Entity> entities) {
    Set<String> tableNames = new HashSet<>();
    for (Entity entity : entities) {
      if (entity.getType() == TABLE) {
        tableNames.add(entity.getTable().getCompleteName());
      }
    }
    return tableNames;
  }

  private static Set<String> getPartitionsFromEntitySet(Set<? extends Entity> entities) {
    Set<String> partitionNames = new HashSet<>();
    for (Entity entity : entities) {
      if (entity.getType() == PARTITION) {
        partitionNames.add(entity.getPartition().getCompleteName());
      }
    }
    return partitionNames;
  }

  private static Set<String> getDatabasesFromEntitySet(Set<? extends Entity> entities) {
    Set<String> databaseNames = new HashSet<>();
    for (Entity entity : entities) {
      if (entity.getType() == DATABASE) {
        databaseNames.add(entity.getDatabase().getName());
      }
    }
    return databaseNames;
  }

  private static String getUser(HookContext hookContext) {
    return hookContext.getUgi().getShortUserName();
  }

  private static String getRequestUser(HookContext hookContext) {
    String requestUser = hookContext.getUserName();
    return requestUser == null ? hookContext.getUgi().getUserName() : requestUser;
  }

  private static ExecutionMode getExecutionMode(QueryPlan plan) {
    // Utilities methods check for null, so possibly it is nullable
    List<Task<?>> rootTasks = ReflectionMethods.INSTANCE.getRootTasks(plan);

    if (rootTasks != null && rootTasks.isEmpty()) {
      return ExecutionMode.CLIENT_ONLY;
    }

    List<TezTask> tezTasks = Utilities.getTezTasks(rootTasks);
    if (!tezTasks.isEmpty()) {
      // Need to go in and check if any of the tasks is running in LLAP mode.
      for (TezTask tezTask : tezTasks) {
        if (tezTask.getWork().getLlapMode()) {
          return ExecutionMode.LLAP;
        }
      }
      return ExecutionMode.TEZ;
    }

    if (!Utilities.getMRTasks(rootTasks).isEmpty()) {
      return ExecutionMode.MR;
    }

    if (!Utilities.getSparkTasks(rootTasks).isEmpty()) {
      return ExecutionMode.SPARK;
    }

    if (TasksRetriever.hasDdlTask(plan.getRootTasks())) {
      return ExecutionMode.DDL;
    }

    return ExecutionMode.NONE;
  }

  private static String getHiveInstanceAddress(HookContext hookContext) {
    String hiveInstanceAddress = hookContext.getHiveInstanceAddress();
    if (hiveInstanceAddress == null) {
      try {
        hiveInstanceAddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        LOG.error("Error trying to get localhost address", e);
      }
    }
    return hiveInstanceAddress;
  }

  private static String getHiveInstanceType(HookContext hookContext) {
    return hookContext.isHiveServerQuery() ? "HS2" : "CLI";
  }
}
