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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hive.common.util.HiveVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Component for Hive version retrieval and validation. */
public class VersionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(VersionValidator.class);

  private static final Pattern pattern = Pattern.compile("^(\\d+)\\.(\\d+).*");

  /**
   * Returns full Hive version string, in {@code major.minor.patch-tags} format.
   *
   * <p>Returning short version string is not possible as {@link HiveVersionInfo#getShortVersion()}
   * is unreliable. For 2.2.0 package it is set to 2.1.0 – <a
   * href="http://go/gh/apache/hive/blob/rel/release-2.2.0/pom.xml#L64">tag source</a>.
   */
  public static String getHiveVersion() {
    // getShortVersion() is unreliable. For 2.2.0 package it is 2.1.0 –
    // http://go/gh/apache/hive/blob/rel/release-2.2.0/pom.xml#L64
    return HiveVersionInfo.getVersion();
  }

  /** Validates is Hive version is within supported versions range */
  public static boolean isHiveVersionSupported(String version) {
    Matcher matcher = pattern.matcher(version);
    if (!matcher.matches()) {
      LOG.warn("Can not parse Hive version '{}', skipping validation", version);
      return false;
    }

    int major = Integer.parseInt(matcher.group(1));
    int minor = Integer.parseInt(matcher.group(2));

    // Covers v2.2.x-v3.x.x
    boolean isVersionValid = major == 2 ? minor >= 2 : major == 3;

    if (!isVersionValid) {
      LOG.error(
          "Current Hive version '{}' is not supported by the Assessment logging hook, logging"
              + " disabled. Please refer to the documentation.",
          version);
    }

    return isVersionValid;
  }

  private VersionValidator() {}
}
