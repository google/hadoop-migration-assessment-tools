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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VersionValidatorTest {

  @Test
  public void isHiveVersionSupported_confirmsSupportedVersions() {
    assertThat(VersionValidator.isHiveVersionSupported("2.2.0")).isTrue();
    assertThat(VersionValidator.isHiveVersionSupported("2.3.8")).isTrue();
    assertThat(VersionValidator.isHiveVersionSupported("2.3")).isTrue();
    assertThat(VersionValidator.isHiveVersionSupported("3.1.3")).isTrue();
  }

  @Test
  public void isHiveVersionSupported_rejectsUnsupportedVersions() {
    assertThat(VersionValidator.isHiveVersionSupported("2.1.9")).isFalse();
    assertThat(VersionValidator.isHiveVersionSupported("1.1.2")).isFalse();
    assertThat(VersionValidator.isHiveVersionSupported("4.0.0")).isFalse();
  }

  @Test
  public void isHiveVersionSupported_rejectsInvalidVersions() {
    assertThat(VersionValidator.isHiveVersionSupported("unexpected")).isFalse();
    assertThat(VersionValidator.isHiveVersionSupported("5")).isFalse();
    assertThat(VersionValidator.isHiveVersionSupported("4.1abc")).isFalse();
  }

  @Test
  public void getHiveVersion_success() {
    assertThat(VersionValidator.getHiveVersion()).isEqualTo("2.2.0");
  }
}
