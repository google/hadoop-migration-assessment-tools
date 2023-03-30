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
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/avro",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/logger",
        "@maven_tests//:com_google_guava_guava",
        "@maven_tests//:com_google_truth_truth",
        "@maven_tests//:junit_junit",
        "@maven_tests//:org_apache_hadoop_hadoop_common",
        "@maven_tests//:org_apache_hadoop_hadoop_mapreduce_client_common",
        "@maven_tests//:org_apache_hadoop_hadoop_yarn_common",
        "@maven_tests//:org_apache_hive_hive_common",
        "@maven_tests//:org_apache_hive_hive_exec",
        "@maven_tests//:org_mockito_mockito_core",
        "@maven_tests//:org_slf4j_slf4j_api",
    ],
)

java_test(
    name = "MigrationAssessmentLoggingHookTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.MigrationAssessmentLoggingHookTest",
    runtime_deps = [
        ":tests",
    ],
)
