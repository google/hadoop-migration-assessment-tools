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

import static org.apache.hadoop.hive.ql.hooks.Entity.Type.PARTITION;
import static org.apache.hadoop.hive.ql.hooks.Entity.Type.TABLE;

import com.google.cloud.bigquery.dwhassessment.hooks.avro.AvroSchemaLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Hook, which captures query events and stores them as {@link GenericRecord}. */
public class MigrationAssessmentLoggingHook implements ExecuteWithHookContext {

  private static final Logger LOG = LoggerFactory.getLogger(MigrationAssessmentLoggingHook.class);

  private static final Schema QUERY_EVENT_SCHEMA = AvroSchemaLoader.loadSchema("QueryEvents.avsc");

  // Just for the example. Real hook will write it to the file
  public List<GenericRecord> records = new ArrayList<>();

  public void run(HookContext hookContext) throws Exception {
    processQueryEventAvro(hookContext);
  }

  private void processQueryEventAvro(HookContext hookContext) {
    QueryPlan plan = hookContext.getQueryPlan();

    LOG.info("Received hook notification for: {}", plan.getQueryId());

    // Make a copy so that we do not modify hookContext conf.
    HiveConf conf = new HiveConf(hookContext.getConf());
    List<ExecDriver> mrTasks = Utilities.getMRTasks(plan.getRootTasks());
    List<TezTask> tezTasks = Utilities.getTezTasks(plan.getRootTasks());
    ExecutionMode executionMode = getExecutionMode(mrTasks, tezTasks);

    GenericRecord queryEvent =
        new GenericRecordBuilder(QUERY_EVENT_SCHEMA)
            .set("QueryId", plan.getQueryId())
            .set("QueryText", plan.getQueryStr())
            .set("EventType", hookContext.getHookType().name())
            .set("Timestamp", plan.getQueryStartTime())
            .set("User", getUser(hookContext))
            .set("RequestUser", getRequestUser(hookContext))
            .set("ExecutionMode", executionMode.name())
            .set("Queue", getQueueName(executionMode, conf))
            .set("TablesRead", getTablesFromEntitySet(plan.getInputs()))
            .set("TablesWritten", getTablesFromEntitySet(plan.getOutputs()))
            .build();
    records.add(queryEvent);
    LOG.info("Processed record: {}", queryEvent);
  }

  private List<String> getTablesFromEntitySet(Set<? extends Entity> entities) {
    List<String> tableNames = new ArrayList<>();
    for (Entity entity : entities) {
      if (entity.getType() == TABLE || entity.getType() == PARTITION) {
        tableNames.add(entity.getTable().getCompleteName());
      }
    }
    return tableNames;
  }

  private String getUser(HookContext hookContext) {
    return hookContext.getUgi().getShortUserName();
  }

  private String getRequestUser(HookContext hookContext) {
    String requestUser = hookContext.getUserName();
    return requestUser == null ? hookContext.getUgi().getUserName() : requestUser;
  }

  private ExecutionMode getExecutionMode(List<ExecDriver> mrTasks, List<TezTask> tezTasks) {
    if (tezTasks.size() > 0) {
      // Need to go in and check if any of the tasks is running in LLAP mode.
      for (TezTask tezTask : tezTasks) {
        if (tezTask.getWork().getLlapMode()) {
          return ExecutionMode.LLAP;
        }
      }
      return ExecutionMode.TEZ;
    }

    return mrTasks.size() > 0 ? ExecutionMode.MR : ExecutionMode.NONE;
  }

  private String getQueueName(ExecutionMode mode, HiveConf conf) {
    switch (mode) {
      case LLAP:
        return conf.get(HiveConf.ConfVars.LLAP_DAEMON_QUEUE_NAME.varname);
      case MR:
        return "TODO_MAPREDUCE";
      case TEZ:
        return "TODO_TEZ";
      case NONE:
      default:
        return null;
    }
  }
}
