<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# TLS test fixtures

Self-signed certificates + PKCS#8 keys used only by the loopback TLS tests
(`connection::tls_tests`, cargo feature `tls`) and the opt-in live TLS test.
Not secrets — the keys never protect anything.

| File | Role |
|---|---|
| `cert.pem` + `key.pem` | server identity (loopback rustls server, live-server keystore) |
| `client-cert.pem` + `client-key.pem` | client identity for the mutual-TLS tests |

The current server certificate expires **2028-11-09**. Regenerate it before
then with the extensions required by the WebPKI verifier:

```sh
cd tests/fixtures/tls
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
  -days 820 -nodes -subj "/CN=localhost" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -addext "extendedKeyUsage=serverAuth" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment"
```

The client certificate (expires **2028-11-09** as well) is a standalone
self-signed client identity. The rustls loopback server explicitly trusts it
and requires it during the mutual-TLS test:

```sh
cd tests/fixtures/tls
openssl req -x509 -newkey rsa:2048 -keyout client-key.pem -out client-cert.pem \
  -days 820 -nodes -subj "/CN=iotdb-client-rust-test-client" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "extendedKeyUsage=clientAuth" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment"
```

## Live TLS server keystore

The end-to-end test (`live_tls_roundtrip`, gated by `IOTDB_TLS_URL`) needs a
TLS-enabled IoTDB. Build a PKCS#12 keystore from the server fixture pair
(regenerate whenever `cert.pem` is regenerated; not checked in):

```sh
cd tests/fixtures/tls
openssl pkcs12 -export -in cert.pem -inkey key.pem \
  -out server-keystore.p12 -name iotdb -passout pass:iotdbtls
```

Then run a throwaway TLS-enabled server and point the test at it
(the docker env-var mechanism appends any lowercase var to
`iotdb-system.properties`; IoTDB ≥ 2.x reads `enable_thrift_ssl`,
`key_store_path`, `key_store_pwd` — client RPC port only, internal
cluster ports stay plain):

```sh
docker run -d --name iotdb-rust-tls-test -p 6668:6667 \
  -e enable_thrift_ssl=true \
  -e key_store_path=/tls/server-keystore.p12 \
  -e key_store_pwd=iotdbtls \
  -v "$PWD/server-keystore.p12":/tls/server-keystore.p12:ro \
  apache/iotdb:2.0.6-standalone

IOTDB_TLS_URL=127.0.0.1:6668 \
IOTDB_TLS_CA=tests/fixtures/tls/cert.pem \
cargo test --features tls live_tls_roundtrip

docker rm -f iotdb-rust-tls-test
```
