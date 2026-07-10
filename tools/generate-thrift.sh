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

# Regenerates Rust Thrift stubs in src/protocol/ from the IoTDB project's IDL.
#
# The Thrift compiler is taken (in order of preference) from:
#   1. $THRIFT_BIN if set
#   2. the IoTDB repo's Maven build output ($IOTDB_REPO, default ../iotdb):
#      iotdb-protocol/*/target/thrift/bin/thrift — run
#      `./mvnw generate-sources -pl iotdb-protocol/thrift-datanode -am` there first.
#      This guarantees the exact Thrift version pinned by the IoTDB pom.
#   3. `thrift` on PATH (fallback; version must match iotdb pom's thrift.version)
#
# IDL files are synced from $IOTDB_REPO before generation when it is available.

set -euo pipefail
cd "$(dirname "$0")/.."

IOTDB_REPO="${IOTDB_REPO:-../iotdb}"

find_thrift() {
  if [[ -n "${THRIFT_BIN:-}" ]]; then echo "$THRIFT_BIN"; return; fi
  local candidate
  candidate=$(ls "$IOTDB_REPO"/iotdb-protocol/*/target/thrift/bin/thrift 2>/dev/null | head -1 || true)
  if [[ -n "$candidate" ]]; then echo "$candidate"; return; fi
  command -v thrift || {
    echo "error: no thrift compiler found. Build iotdb-protocol first:" >&2
    echo "  (cd $IOTDB_REPO && ./mvnw generate-sources -pl iotdb-protocol/thrift-datanode -am)" >&2
    exit 1
  }
}

THRIFT=$(find_thrift)
echo "Using thrift: $THRIFT ($($THRIFT --version))"

# Sync IDL from the IoTDB repo when present
if [[ -d "$IOTDB_REPO/iotdb-protocol" ]]; then
  cp "$IOTDB_REPO/iotdb-protocol/thrift-datanode/src/main/thrift/client.thrift" thrift/
  cp "$IOTDB_REPO/iotdb-protocol/thrift-commons/src/main/thrift/common.thrift" thrift/
  echo "Synced IDL from $IOTDB_REPO/iotdb-protocol"
fi

"$THRIFT" --gen rs -out src/protocol thrift/common.thrift
"$THRIFT" --gen rs -out src/protocol thrift/client.thrift

# The generator assumes included modules live at the crate root; ours are under
# src/protocol/. Rewrite the cross-module path.
sed -i '' 's/use crate::common;/use crate::protocol::common;/' src/protocol/client.rs

# Re-prepend the Apache license header (the generator emits bare files).
for f in src/protocol/common.rs src/protocol/client.rs; do
  if ! head -30 "$f" | grep -q "Licensed to the Apache Software Foundation"; then
    tmp=$(mktemp)
    sed 's/^/\/\/ /; s/ *$//' tools/license-header.txt > "$tmp"
    printf '\n' >> "$tmp"
    cat "$f" >> "$tmp"
    mv "$tmp" "$f"
  fi
done

echo "Generated src/protocol/{common,client}.rs"
