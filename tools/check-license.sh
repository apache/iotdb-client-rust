#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Checks that every source file carries the Apache 2.0 license header.
# Exits 1 listing the offenders, 0 when all files are compliant.

set -euo pipefail
cd "$(dirname "$0")/.."

MARKER="Licensed to the Apache Software Foundation"
offenders=()

check() {
  local f="$1"
  # The header must appear near the top of the file (first 30 lines).
  if ! head -30 "$f" | grep -q "$MARKER"; then
    offenders+=("$f")
  fi
}

# Rust sources (including generated protocol stubs), examples and tests.
while IFS= read -r f; do check "$f"; done < <(find src examples tests -name '*.rs' 2>/dev/null)

# Build / CI / infra files.
for f in Cargo.toml docker-compose*.yml .github/workflows/*.yml tools/*.sh thrift/*.thrift; do
  [[ -f "$f" ]] && check "$f"
done

if ((${#offenders[@]})); then
  echo "Files missing the Apache 2.0 license header:" >&2
  printf '  %s\n' "${offenders[@]}" >&2
  exit 1
fi
echo "License header check passed."
