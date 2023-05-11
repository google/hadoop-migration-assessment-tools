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

package com.google.cloud.bigquery.dwhassessment.hooks.logger.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;

/** Helper for retrieving tasks of the target type from the query plan tasks. */
public final class TasksRetriever {
  private TasksRetriever() {}

  public static List<DDLTask> getDdlTasks(List<Task<? extends Serializable>> tasks) {
    List<DDLTask> ddlTasks = new ArrayList<>();
    if (tasks != null) {
      getDdlTasksRecursively(tasks, ddlTasks);
    }

    return ddlTasks;
  }

  private static void getDdlTasksRecursively(
      List<Task<? extends Serializable>> tasks, List<DDLTask> ddlTasks) {

    for (Task<? extends Serializable> task : tasks) {
      if (task instanceof DDLTask && !ddlTasks.contains(task)) {
        ddlTasks.add((DDLTask) task);
      }

      if (task.getDependentTasks() != null) {
        getDdlTasksRecursively(task.getDependentTasks(), ddlTasks);
      }
    }
  }
}
