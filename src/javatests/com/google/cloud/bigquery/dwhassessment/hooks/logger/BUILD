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

java_plugin(
    name = "autovalue_plugin",
    processor_class = "com.google.auto.value.processor.AutoValueProcessor",
    deps = [
        "@maven_tests//:com_google_auto_value_auto_value",
    ],
)

java_library(
    name = "autovalue",
    exported_plugins = [
        ":autovalue_plugin",
    ],
    neverlink = 1,
    exports = [
        "@maven_tests//:com_google_auto_value_auto_value",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = glob(["*.java"]),
    deps = [
        ":autovalue",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/avro",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/logger",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/testing",
        "@maven_tests//:com_google_auto_value_auto_value",
        "@maven_tests//:com_google_auto_value_auto_value_annotations",
        "@maven_tests//:com_google_guava_guava",
        "@maven_tests//:com_google_truth_extensions_truth_java8_extension",
        "@maven_tests//:com_google_truth_truth",
        "@maven_tests//:junit_junit",
        "@maven_tests//:org_apache_hadoop_hadoop_common",
        "@maven_tests//:org_apache_hadoop_hadoop_mapred",
        "@maven_tests//:org_apache_hadoop_hadoop_mapreduce_client_common",
        "@maven_tests//:org_apache_hadoop_hadoop_yarn_api",
        "@maven_tests//:org_apache_hadoop_hadoop_yarn_client",
        "@maven_tests//:org_apache_hadoop_hadoop_yarn_common",
        "@maven_tests//:org_apache_hive_hive_common",
        "@maven_tests//:org_apache_hive_hive_exec",
        "@maven_tests//:org_apache_tez_tez_api",
        "@maven_tests//:org_apache_tez_tez_dag",
        "@maven_tests//:org_mockito_mockito_core",
        "@maven_tests//:org_slf4j_slf4j_api",
    ],
)

java_test(
    name = "EventRecordConstructorTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.EventRecordConstructorTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "RecordsWriterTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.RecordsWriterTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "DatePartitionedRecordsWriterFactoryTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.DatePartitionedRecordsWriterFactoryTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "EventLoggerTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.EventLoggerTest",
    runtime_deps = [
        ":tests",
    ],
)

java_test(
    name = "ApplicationIdRetrieverTest",
    size = "small",
    test_class = "com.google.cloud.bigquery.dwhassessment.hooks.logger.ApplicationIdRetrieverTest",
    runtime_deps = [
        ":tests",
    ],
)
