# Copyright 2022-2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
load("@rules_java//java:defs.bzl", "java_library", "java_test")

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["*.java"]),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/logger/utils",
        "@maven_tests//:com_google_guava_guava",
        "@maven_tests//:com_google_truth_truth",
        "@maven_tests//:commons_lang_commons_lang",
        "@maven_tests//:junit_junit",
        "@maven_tests//:org_apache_hive_hive_exec",
    ],
)

java_test(
    name = "IdGeneratorTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.IdGeneratorTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "TasksRetrieverTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.TasksRetrieverTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "VersionValidatorTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.utils.VersionValidatorTest",
    runtime_deps = [
        ":tests",
    ],
)
