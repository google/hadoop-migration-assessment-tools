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
package com.google.cloud.bigquery.dwhassessment.hooks;

import static com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggingHookConstants.QUERY_EVENT_SCHEMA;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.dwhassessment.hooks.logger.EventLogger;
import com.google.cloud.bigquery.dwhassessment.hooks.logger.LoggerVarsConfig;
import com.google.cloud.bigquery.dwhassessment.hooks.test_utils.TestUtils;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class MigrationAssessmentLoggingHookTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Mock Hive hiveMock;

  private HiveConf conf;
  private String tmpFolder;
  private HookContext hookContext;
  private QueryState queryState;

  @Before
  public void setup() throws Exception {
    TestUtils utils = new TestUtils(hiveMock);
    conf = utils.getConf();
    queryState = utils.getQueryState();
    hookContext = utils.getHookContext();

    tmpFolder = folder.newFolder().getAbsolutePath();
    conf.set(LoggerVarsConfig.HIVE_QUERY_EVENTS_BASE_PATH.getConfName(), tmpFolder);
  }

  @After
  public void tearDown() {
    queryState.setCommandType(null);
  }

  @Test
  public void run_success() throws Exception {
    hookContext.setHookType(HookType.PRE_EXEC_HOOK);
    queryState.setCommandType(HiveOperation.QUERY);
    MigrationAssessmentLoggingHook hook = new MigrationAssessmentLoggingHook();

    // Act
    hook.run(hookContext);
    EventLogger.getInstance(conf, Clock.systemUTC()).shutdown();

    // Assert
    List<GenericRecord> records = TestUtils.readOutputRecords(conf, tmpFolder);
    assertThat(records).containsExactly(TestUtils.getPreExecRecord());
  }


}
