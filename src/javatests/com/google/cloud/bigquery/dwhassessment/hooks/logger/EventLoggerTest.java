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

import com.google.cloud.bigquery.dwhassessment.hooks.testing.TestUtils;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
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
public class EventLoggerTest {

  @Rule public MockitoRule mocks = MockitoJUnit.rule();
  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Mock Hive hiveMock;

  private String tmpFolder;
  private HookContext hookContext;
  private HiveConf conf;
  private QueryState queryState;

  private EventLogger logger;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    tmpFolder = folder.newFolder().getAbsolutePath();
    conf.set(LoggerVarsConfig.HIVE_QUERY_EVENTS_BASE_PATH.getConfName(), tmpFolder);

    queryState = new QueryState(conf);
    hookContext = TestUtils.createDefaultHookContext(hiveMock, queryState);

    logger = new EventLogger(conf, TestUtils.createFixedClock());
  }

  @Test
  public void preExecHook_success() throws Exception {
    hookContext.setHookType(HookType.PRE_EXEC_HOOK);
    queryState.setCommandType(HiveOperation.QUERY);

    // Act
    logger.handle(hookContext);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = TestUtils.readOutputRecords(conf, tmpFolder);
    assertThat(records).containsExactly(TestUtils.createPreExecRecord());
  }

  @Test
  public void postExecHook_success() throws Exception {
    hookContext.setHookType(HookType.POST_EXEC_HOOK);

    // Act
    logger.handle(hookContext);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = TestUtils.readOutputRecords(conf, tmpFolder);
    assertThat(records).containsExactly(TestUtils.createPostExecRecord(EventStatus.SUCCESS));
  }

  @Test
  public void onFailureHook_success() throws Exception {
    hookContext.setHookType(HookType.ON_FAILURE_HOOK);

    // Act
    logger.handle(hookContext);
    logger.shutdown();

    // Assert
    List<GenericRecord> records = TestUtils.readOutputRecords(conf, tmpFolder);
    assertThat(records).containsExactly(TestUtils.createPostExecRecord(EventStatus.FAIL));;
  }
}
