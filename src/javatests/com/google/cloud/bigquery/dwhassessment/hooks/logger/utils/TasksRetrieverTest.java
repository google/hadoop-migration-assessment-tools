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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.CopyTask;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TasksRetrieverTest {

  @Test
  public void hasDdlTask_withDdlTaskWithoutDependentTasks() {
    List<Task<? extends Serializable>> tasks = ImmutableList.of(new DDLTask(), new CopyTask());

    assertThat(TasksRetriever.hasDdlTask(tasks)).isTrue();
  }

  @Test
  public void hasDdlTask_withoutDdlTaskWithoutDependentTasks() {
    List<Task<? extends Serializable>> tasks = ImmutableList.of(new CopyTask());

    assertThat(TasksRetriever.hasDdlTask(tasks)).isFalse();
  }

  @Test
  public void hasDdlTask_withDdlTaskInDependentTasks() {
    Task<? extends Serializable> task = new CopyTask();
    task.setChildTasks(ImmutableList.of(new DDLTask()));
    List<Task<? extends Serializable>> tasks = ImmutableList.of(task);

    assertThat(TasksRetriever.hasDdlTask(tasks)).isTrue();
  }
}
