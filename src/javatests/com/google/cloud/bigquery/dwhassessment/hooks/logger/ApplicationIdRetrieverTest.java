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

import static com.google.cloud.bigquery.dwhassessment.hooks.testing.TestUtils.createDefaultSessionState;
import static com.google.cloud.bigquery.dwhassessment.hooks.testing.TestUtils.createMapRedStats;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionState;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.TezClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ApplicationIdRetrieverTest {

  private HiveConf conf;

  @Before
  public void setup() {
    conf = new HiveConf();
  }

  @Test
  public void determineLlapApplicationId_unsetHostsConfigValue() {
    // Act
    List<ApplicationId> applicationId =
        ApplicationIdRetriever.determineLlapApplicationId(conf, ExecutionMode.LLAP);

    // Assert
    assertThat(applicationId).isEmpty();
  }

  @Test
  public void determineApplicationId_unsupportedExecutionMode() {
    // Act
    List<ApplicationId> applicationId =
        ApplicationIdRetriever.determineApplicationId(conf, ExecutionMode.DDL);

    // Assert
    assertThat(applicationId).isEmpty();
  }

  @Test
  public void determineApplicationId_MapReduce_success() {
    SessionState sessionState = createDefaultSessionState(conf);

    sessionState.getMapRedStats().put("Stage-1", createMapRedStats("job_1685098059769_1951"));
    sessionState.getMapRedStats().put("Stage-2", createMapRedStats("job_1685098059769_1949"));
    List<ApplicationId> expectedList = new ArrayList<>();
    expectedList.add(ApplicationId.newInstance(/* clusterTimestamp= */ 1685098059769L, /* id= */ 1951));
    expectedList.add(ApplicationId.newInstance(/* clusterTimestamp= */ 1685098059769L, /* id= */ 1949));

    // Act
    List<ApplicationId> applicationIds =
        ApplicationIdRetriever.determineApplicationId(conf, ExecutionMode.MR);

    // Assert
    assertThat(applicationIds).isEqualTo(expectedList);
  }

  @Test
  public void determineApplicationId_MapReduce_malformedJobId() {
    SessionState sessionState = createDefaultSessionState(conf);

    sessionState.getMapRedStats().put("Stage-1", createMapRedStats("malformed_job_id"));

    // Act
    List<ApplicationId> applicationId =
        ApplicationIdRetriever.determineApplicationId(conf, ExecutionMode.MR);

    // Assert
    assertThat(applicationId).isEmpty();
  }

  @Test
  public void determineApplicationId_TezWithEmptyState() {
    // Act
    List<ApplicationId> applicationId =
        ApplicationIdRetriever.determineApplicationId(conf, ExecutionMode.TEZ);

    // Assert
    assertThat(applicationId).isEmpty();
  }

  @Test
  public void determineApplicationId_TezWithAppIdInSessionState() {
    ApplicationId expectedApplicationId =
        ApplicationId.newInstance(/* clusterTimestamp= */ 123, /* id= */ 456);
    SessionState sessionState = createDefaultSessionState(conf);
    TezSessionState tezSessionState = mock(TezSessionState.class);
    TezClient tezClient = mock(TezClient.class);
    when(tezClient.getAppMasterApplicationId()).thenReturn(expectedApplicationId);
    when(tezSessionState.getSession()).thenReturn(tezClient);
    sessionState.setTezSession(tezSessionState);

    // Act
    List<ApplicationId> applicationId =
        ApplicationIdRetriever.determineApplicationId(conf, ExecutionMode.TEZ);

    // Assert
    assertThat(applicationId).containsExactly(expectedApplicationId);
  }
}
