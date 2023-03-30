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
import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.PARTITION;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.json.JSONObject;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Constructor for generic records for given hook event. */
public class EventRecordConstructor {

  private static final Logger LOG = LoggerFactory.getLogger(EventRecordConstructor.class);

  private final Clock clock;

  public EventRecordConstructor(Clock clock) {
    this.clock = clock;
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

    LOG.warn("Ignoring event of type: {}", hookContext.getHookType());
    return Optional.empty();
  }

  private GenericRecord getPreHookEvent(HookContext hookContext) {
    QueryPlan plan = hookContext.getQueryPlan();

    // Make a copy so that we do not modify hookContext conf.
    HiveConf conf = new HiveConf(hookContext.getConf());
    List<ExecDriver> mrTasks = Utilities.getMRTasks(plan.getRootTasks());
    List<TezTask> tezTasks = Utilities.getTezTasks(plan.getRootTasks());
    ExecutionMode executionMode = getExecutionMode(mrTasks, tezTasks);

    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", plan.getQueryId())
        .set("QueryText", plan.getQueryStr())
        .set("EventType", EventType.QUERY_SUBMITTED.name())
        .set("StartTime", plan.getQueryStartTime())
        .set("UserName", getUser(hookContext))
        .set("RequestUser", getRequestUser(hookContext))
        .set("ExecutionMode", executionMode.name())
        .set("Queue", getQueueName(executionMode, conf))
        .set("TablesRead", getTablesFromEntitySet(plan.getInputs()))
        .set("TablesWritten", getTablesFromEntitySet(plan.getOutputs()))
        .set("IsMapReduce", mrTasks.size() > 0)
        .set("IsTez", tezTasks.size() > 0)
        .set("SessionId", hookContext.getSessionId())
        .set("InvokerInfo", conf.getLogIdVar(hookContext.getSessionId()))
        .set("ThreadName", hookContext.getThreadId())
        .set("ClientIpAddress", hookContext.getIpAddress())
        .set("ClientIpAddress", hookContext.getIpAddress())
        .set("HookVersion", HOOK_VERSION)
        .set("HiveAddress", getHiveInstanceAddress(hookContext))
        .set("HiveInstanceType", getHiveInstanceType(hookContext))
        .set("LlapApplicationId", determineLlapId(conf, executionMode))
        .set("MapReduceCountersObject", dumpMapReduceCounters(mrTasks))
        .set("TezCountersObject", dumpTezCounters(tezTasks))
        .set("OperationId", hookContext.getOperationId())
        .build();
  }

  private GenericRecord getPostHookEvent(HookContext hookContext, EventStatus status) {
    QueryPlan plan = hookContext.getQueryPlan();
    LOG.info("Received post-hook notification for: {}", plan.getQueryId());

    List<ExecDriver> mrTasks = Utilities.getMRTasks(plan.getRootTasks());
    List<TezTask> tezTasks = Utilities.getTezTasks(plan.getRootTasks());

    return new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
        .set("QueryId", plan.getQueryId())
        .set("EventType", EventType.QUERY_COMPLETED.name())
        .set("EndTime", clock.millis())
        .set("UserName", getUser(hookContext))
        .set("RequestUser", getRequestUser(hookContext))
        .set("Status", status.name())
        .set("ErrorMessage", hookContext.getErrorMessage())
        .set("HookVersion", HOOK_VERSION)
        .set("PerfObject", dumpPerfData(hookContext.getPerfLogger()))
        .set("MapReduceCountersObject", dumpMapReduceCounters(mrTasks))
        .set("TezCountersObject", dumpTezCounters(tezTasks))
        .set("OperationId", hookContext.getOperationId())
        .build();
  }

  private static String dumpMapReduceCounters(List<ExecDriver> mrTasks) {
    return new JSONArray(mrTasks.stream().map(ExecDriver::getCounters).collect(Collectors.toList()))
        .toString();
  }

  /**
   * Tez tasks counters is a deeply nested set of key - value pairs. This attempts to dump them as
   * is, preserving their original structure.
   */
  private static String dumpTezCounters(List<TezTask> tezTasks) {
    JSONArray outerObj = new JSONArray();

    for (TezTask tezTask : tezTasks) {
      TezCounters tezCounters = tezTask.getTezCounters();
      if (tezCounters == null) {
        continue;
      }

      JSONArray taskObj = new JSONArray();

      tezCounters.forEach(
          counterGroup -> {
            JSONObject groupCounters =
                new JSONObject(
                    StreamSupport.stream(counterGroup.spliterator(), false)
                        .collect(Collectors.toMap(TezCounter::getName, TezCounter::getValue)));
            JSONObject counterGroupData =
                new JSONObject().put(counterGroup.getDisplayName(), groupCounters);

            taskObj.put(counterGroupData);
          });

      outerObj.put(taskObj);
    }

    return outerObj.toString();
  }

  private static String dumpPerfData(PerfLogger perfLogger) {
    JSONObject perfObj = new JSONObject();

    for (String key : perfLogger.getEndTimes().keySet()) {
      perfObj.put(key, perfLogger.getDuration(key));
    }

    return perfObj.toString();
  }

  private static List<String> getTablesFromEntitySet(Set<? extends Entity> entities) {
    List<String> tableNames = new ArrayList<>();
    for (Entity entity : entities) {
      if (entity.getType() == TABLE || entity.getType() == PARTITION) {
        tableNames.add(entity.getTable().getCompleteName());
      }
    }
    return tableNames;
  }

  private static String getUser(HookContext hookContext) {
    return hookContext.getUgi().getShortUserName();
  }

  private static String getRequestUser(HookContext hookContext) {
    String requestUser = hookContext.getUserName();
    return requestUser == null ? hookContext.getUgi().getUserName() : requestUser;
  }

  private static ExecutionMode getExecutionMode(List<ExecDriver> mrTasks, List<TezTask> tezTasks) {
    if (tezTasks.size() > 0) {
      // Need to go in and check if any of the tasks is running in LLAP mode.
      for (TezTask tezTask : tezTasks) {
        if (tezTask.getWork().getLlapMode()) {
          return ExecutionMode.LLAP;
        }
      }
      return ExecutionMode.TEZ;
    } else if (mrTasks.size() > 0) {
      return ExecutionMode.MR;
    } else {
      return ExecutionMode.NONE;
    }
  }

  private static String getQueueName(ExecutionMode mode, HiveConf conf) {
    switch (mode) {
      case LLAP:
        return conf.get(HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.varname);
      case MR:
        return conf.get(MR_QUEUE_NAME.getConfName());
      case TEZ:
        return conf.get(TEZ_QUEUE_NAME.getConfName());
      case NONE:
      default:
        return null;
    }
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

  private static String determineLlapId(HiveConf conf, ExecutionMode mode) {
    // Note: for now, LLAP is only supported in Tez tasks. Will never come to MR; others may
    // be added here, although this is only necessary to have extra debug information.
    if (mode == ExecutionMode.LLAP) {
      // In HS2, the client should have been cached already for the common case.
      // Otherwise, this may actually introduce delay to compilation for the first query.
      String hosts = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
      if (hosts != null && !hosts.isEmpty()) {
        try {
          return LlapRegistryService.getClient(conf).getApplicationId().toString();
        } catch (IOException e) {
          LOG.error("Error trying to get llap instance. Hosts: {}", hosts, e);
        }
      } else {
        LOG.info("Cannot determine LLAP instance on client - service hosts are not set");
      }
    }

    return null;
  }
}
