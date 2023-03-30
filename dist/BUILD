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

load("@rules_pkg//:mappings.bzl", "pkg_attributes", "pkg_filegroup", "pkg_files", "strip_prefix")
load("@rules_pkg//:pkg.bzl", "pkg_zip")

pkg_files(
    name = "hooks_files",
    srcs = [
        "//:LICENSE",
        "//:README.md",
        "//src/java/com/google/cloud/bigquery/dwhassessment/hooks:HiveMigrationAssessmentQueryLogsHooks_deploy.jar",
    ],
    attributes = pkg_attributes(
        mode = "0555",
    ),
    prefix = "",
)

pkg_zip(
    name = "hadoop-migration-assessment-hooks",
    srcs = [
        ":hooks_files",
    ],
    package_dir = "hadoop-migration-assessment-hooks",
)
