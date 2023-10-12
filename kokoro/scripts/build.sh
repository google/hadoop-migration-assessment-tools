#!/bin/bash

# Fail on any error.
set -e
cd ../../

bazel test //...
bazel build //dist:all
