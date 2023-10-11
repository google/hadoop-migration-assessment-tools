#!/bin/bash

# Fail on any error.
set -e
cd /hadoop-migration-assessment-tools
bazel build //dist:all
