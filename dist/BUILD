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
