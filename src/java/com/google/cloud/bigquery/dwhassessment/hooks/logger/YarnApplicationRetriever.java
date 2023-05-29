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

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper for retrieving YARN Application Report by its ID. */
public class YarnApplicationRetriever {

  private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationRetriever.class);

  public YarnApplicationRetriever() {}

  public Optional<ApplicationReport> retrieve(HiveConf conf, ApplicationId applicationId) {
    try (YarnClient yarnClient = YarnClient.createYarnClient()) {
      yarnClient.init(conf);
      yarnClient.start();
      ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
      yarnClient.stop();

      return Optional.of(applicationReport);
    } catch (IOException | YarnException e) {
      LOG.warn("Can not retrieve application report for Application ID '{}'", applicationId);
      return Optional.empty();
    }
  }
}
