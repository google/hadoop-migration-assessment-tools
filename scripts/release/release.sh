#!/bin/bash
# Copyright 2023 Google LLC
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
# limitations under the License
#
# A manual build script to build release and update github repo
#
# Display commands being run.
# WARNING: please only enable 'set -x' if necessary for debugging, and be very
#  careful if you handle credentials (e.g. from Keystore) with 'set -x':
#  statements like "export VAR=$(cat /tmp/keystore/credentials)" will result in
#  the credentials being printed in build logs.
#  Additionally, recursive invocation with credentials as command-line
#  parameters, will print the full command, with credentials, in the build logs.
# set -x

# Fail on any error.
set -e
# Fail on errors in pipes.
set -o pipefail

echo Enter client id:
read -s -r GIT_CLIENT_ID
echo Enter token:
read -s -r GIT_CLIENT_TOKEN

#Variables
export SCRIPT=$(realpath "$0")
export RELEASE_SCRIPT_DIR="$(dirname "$SCRIPT")"
export SCRIPT_PARENT_DIR="$(dirname -- "$RELEASE_SCRIPT_DIR")"
export REPO_DIR="$(dirname -- "$SCRIPT_PARENT_DIR")"
export RELEASE_OUTPUT_FILE="${REPO_DIR}/bazel-bin/dist/hadoop-migration-assessment-hooks.zip"
export BUILD_SCRIPT="$RELEASE_SCRIPT_DIR/build.sh"
source "${RELEASE_SCRIPT_DIR}/release_utils.sh"

cd "${REPO_DIR}"

export LAST_GIT_TAG="$(git tag |
  grep -E '^v[0-9]' |
  sort -V |
  tail -1)"

# Do we already know what version we want to release?
if [[ -z "${USE_TAG}" ]]; then
  if [[ -z "${LAST_GIT_TAG}" ]]; then
    err "No previous git tag found and it was not provided with USE_TAG env"
  fi
  VERSION="$(increment_tag_version ${LAST_GIT_TAG})"
  log "Will create new version '${VERSION}'"

  if [ "$(git tag -l ${VERSION})" ]; then
    err "ERROR! Tag for '${VERSION}' already exists!"
  fi

  code="$(http_get_error_code "Accept: application/vnd.github.v3+json" \
    "https://api.github.com/repos/google/hadoop-migration-assessment-tools/releases/tags/${VERSION}")"
  if [ "${code}" != "404" ]; then
    err "ERROR! Release with '${VERSION}' tag version name already exists or http failed! Http code is ${code}"
  fi
else
  VERSION="${USE_TAG}"
  if [[ "${LAST_RELEASE_TAG}" ]]; then
    LAST_GIT_TAG=${LAST_RELEASE_TAG}
  fi
fi

log "Version name '${VERSION}' verified"

# Run build and integration tests
cd "${SCRIPT_WORKING_DIR}"
log "Build script: '${BUILD_SCRIPT}', current dir '$(pwd)'"
bash "${BUILD_SCRIPT}"

# revert to initial state after running build script
cd "${REPO_DIR}"

log "Prepare release notes"

output="$(http_post_check_status "200" "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/repos/google/hadoop-migration-assessment-tools/releases/generate-notes" \
  "$(cat << EOF
{
  "tag_name": "${VERSION}",
  "previous_tag_name": "${LAST_GIT_TAG}"
}
EOF
)"
)"
release_body="$(jq -r '.body' <<< "$output")"

log "Create release"

payload="$(
  jq --null-input \
    --arg tag "${VERSION}" \
    --arg name "${VERSION}" \
    --arg body "${release_body}" \
    '{ tag_name: $tag, name: $name, body: $body, draft: false }'
)"

# Create new release as a draft
response="$(http_post_check_status "201" "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/repos/google/hadoop-migration-assessment-tools/releases" "${payload}")"

log "Append zip file to the release"

# Attach release binary file to the release
release_id="$(jq -r '.id' <<<"${response}")"
curl \
  --data-binary @"${RELEASE_OUTPUT_FILE}" \
  --user "${GIT_CLIENT_ID}:${GIT_CLIENT_TOKEN}" \
  -H "Content-Type: application/octet-stream" \
  "https://uploads.github.com/repos/google/hadoop-migration-assessment-tools/releases/${release_id}/assets?name=hadoop-migration-assessment-hooks-${VERSION}.zip"

log "Release was published"

# Pull just published tag
git pull
