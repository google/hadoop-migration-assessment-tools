# Copyright 2022 Google LLC
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
load("@rules_java//java:defs.bzl", "java_library")

package(default_visibility = ["//src:internal"])

java_library(
    name = "logger",
    srcs = glob(["*.java"]),
    deps = [
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks/avro",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_avro_avro",
        "@maven//:org_apache_commons_commons_compress",
        "@maven//:org_apache_hadoop_hadoop_common",
        "@maven//:org_apache_hive_hive_exec_2_2_0",
        "@maven//:org_slf4j_slf4j_api",
    ],
)
