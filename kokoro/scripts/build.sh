#!/bin/bash

# Fail on any error.
set -e
cd ../../
bazel build //dist:all
