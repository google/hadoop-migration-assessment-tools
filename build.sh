#!/bin/bash

# Fail on any error.
set -e

bazel build //dist:all cd bazel-bin/dist/ 