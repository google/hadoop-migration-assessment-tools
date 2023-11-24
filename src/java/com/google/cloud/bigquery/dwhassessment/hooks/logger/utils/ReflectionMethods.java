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

import com.google.cloud.bigquery.dwhassessment.hooks.logger.exception.LoggingHookFatalException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.log.PerfLogger;

/**
 * Component to handle methods and classes, which might differ between Hive versions, with
 * reflection.
 *
 * <p>Although all hive-exec 2.2.x-3.1.3 open source releases are compatible for methods used by the
 * logging hook, we saw multiple vendors using their own versions of Hive with version 3.1.3x, but
 * using at least part of the codebase shipped in 4.0.0. Normally, the changes are minor, but since
 * Java compiles the logging hook JAR against specific signatures, we need to make sure we can
 * handle cases when a function returns either HashSet or Set.
 */
public final class ReflectionMethods {

  public static final ReflectionMethods INSTANCE = new ReflectionMethods();

  private final Method queryPlanGetRootTasks;
  private final Method queryPlanGetInputs;
  private final Method queryPlanGetOutputs;
  private final Method perfLoggerGetStartTimes;
  private final Class<?> ddlTaskClass;

  private ReflectionMethods() {
    try {
      Class<?> queryPlan = Class.forName("org.apache.hadoop.hive.ql.QueryPlan");
      queryPlanGetRootTasks = queryPlan.getMethod("getRootTasks");
      queryPlanGetInputs = queryPlan.getMethod("getInputs");
      queryPlanGetOutputs = queryPlan.getMethod("getOutputs");

      Class<?> perfLogger = Class.forName("org.apache.hadoop.hive.ql.log.PerfLogger");
      perfLoggerGetStartTimes = perfLogger.getMethod("getStartTimes");
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new LoggingHookFatalException("Failed to initialize methods with reflection", e);
    }

    Class<?> ddlTaskClassTemp;
    try {
      ddlTaskClassTemp = Class.forName("org.apache.hadoop.hive.ql.exec.DDLTask");
    } catch (ClassNotFoundException unused) {
      try {
        ddlTaskClassTemp = Class.forName("org.apache.hadoop.hive.ql.ddl.DDLTask");
      } catch (ClassNotFoundException e) {
        throw new LoggingHookFatalException("Failed to find DDLTask class with reflection", e);
      }
    }
    ddlTaskClass = ddlTaskClassTemp;
  }

  /**
   * Returns {@code QueryPlan#getRootTasks()} invocation result. Some Hive versions return list of
   * type {@code Task<? extends java.io.Serializable>} and some - {@code Task<?>}.
   */
  public List<Task<?>> getRootTasks(QueryPlan queryPlan) {
    return (List<Task<?>>) invokeChecked(queryPlanGetRootTasks, queryPlan);
  }

  /**
   * Returns {@code QueryPlan#getInputs()} invocation result. Some Hive versions return {@code
   * java.util.HashSet} and some - {@code java.util.Set}.
   */
  public Set<ReadEntity> getInputs(QueryPlan queryPlan) {
    return (Set<ReadEntity>) invokeChecked(queryPlanGetInputs, queryPlan);
  }

  /**
   * Returns {@code QueryPlan#getOutputs()} invocation result. Some Hive versions return {@code
   * java.util.HashSet} and some - {@code java.util.Set}.
   */
  public Set<WriteEntity> getOutputs(QueryPlan queryPlan) {
    return (Set<WriteEntity>) invokeChecked(queryPlanGetOutputs, queryPlan);
  }

  /**
   * Returns {@code PerfLogger#getStartTimes()} invocation result. Some Hive versions return {@code
   * com.google.common.collect.ImmutableMap} and some - {@code java.util.Map}.
   */
  public Map<String, Long> getStartTimes(PerfLogger perfLogger) {
    return (Map<String, Long>) invokeChecked(perfLoggerGetStartTimes, perfLogger);
  }

  /**
   * Returns {@code DDLTask} class. In Hive v4 it was moved from {@code
   * org.apache.hadoop.hive.ql.exec} to {@code org.apache.hadoop.hive.ql.ddl} package.
   */
  public Class<?> getDdlTaskClass() {
    return ddlTaskClass;
  }

  private Object invokeChecked(Method method, Object target) {
    try {
      return method.invoke(target);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new LoggingHookFatalException("Failed to invoke method with reflection", e);
    }
  }
}
