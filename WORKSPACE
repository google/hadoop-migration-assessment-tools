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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_java",
    sha256 = "bcfabfb407cb0c8820141310faa102f7fb92cc806b0f0e26a625196101b0b57e",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/5.5.0/rules_java-5.5.0.tar.gz",
    ],
)

load("@rules_java//java:repositories.bzl", "remote_jdk8_repos")

remote_jdk8_repos()

RULES_JVM_EXTERNAL_TAG = "4.0"

RULES_JVM_EXTERNAL_SHA = "31701ad93dbfe544d597dbe62c9a1fdd76d81d8a9150c2bf1ecf928ecdf97169"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@rules_jvm_external//:specs.bzl", "maven")

# All artifacts here should have "neverlink = True" unless it's really required to distribute them
maven_install(
    artifacts = [
        maven.artifact(
            "com.google.guava",
            "guava",
            "31.0-jre",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.commons",
            "commons-compress",
            "1.10",
            neverlink = True,
        ),
        maven.artifact(
            "commons-lang",
            "commons-lang",
            "2.6",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-common",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-yarn-common",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-yarn-api",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-yarn-client",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.avro",
            "avro",
            "1.10.2",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.tez",
            "tez-api",
            "0.8.5",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.tez",
            "tez-common",
            "0.8.5",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.curator",
            "apache-curator",
            "2.7.1",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hive",
            "hive-exec",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-mapred",
            "0.22.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.apache.hadoop",
            "hadoop-mapreduce-client-common",
            "2.2.0",
            neverlink = True,
        ),
        maven.artifact(
            "org.slf4j",
            "slf4j-api",
            "1.7.10",
            neverlink = True,
        ),
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
    version_conflict_policy = "pinned",
)

# Tests need dependencies in the runtime. Since there is no way to mark the dependency as both
# "neverlink" and "testonly", create a separate declaration specifically for tests.
# Code in java/ folder must not depend on this declaration.
maven_install(
    name = "maven_tests",
    artifacts = [
        "org.apache.hadoop:hadoop-common:2.9.0",
        "org.apache.hadoop:hadoop-yarn-common:2.2.0",
        "org.apache.hadoop:hadoop-yarn-api:2.2.0",
        "org.apache.hadoop:hadoop-yarn-client:2.2.0",
        "org.apache.hadoop:hadoop-mapreduce-client-common:2.9.0",
        "org.apache.hadoop:hadoop-mapreduce-client-core:2.9.0",
        "org.apache.hive:hive-exec:2.2.0",
        "org.apache.hadoop:hadoop-mapred:0.22.0",
        "org.apache.tez:tez-dag:0.8.5",
        "org.apache.tez:tez-api:0.8.5",
        "org.apache.tez:tez-common:0.8.5",
        "commons-lang:commons-lang:2.6",
        "com.google.auto.value:auto-value:1.8.2",
        "com.google.auto.value:auto-value-annotations:1.8.2",
        "com.google.guava:guava:29.0-jre",
        "com.google.truth:truth:1.1.3",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "org.apache.curator:apache-curator:2.7.1",
        "junit:junit:4.13.2",
        "org.mockito:mockito-core:3.11.1",
        "org.slf4j:slf4j-api:1.7.10",
    ],
    excluded_artifacts = [
        "org.pentaho:pentaho-aggdesigner-algorithm",
    ],
    fetch_sources = True,
    repositories = [
        "https://maven.google.com",
        "https://repo1.maven.org/maven2",
    ],
)

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()
