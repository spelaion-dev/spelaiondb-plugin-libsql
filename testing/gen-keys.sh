#!/usr/bin/env bash
# Generates the test keys (they are not real secrets; they are in .gitignore).
#   keys/ca.crt      test certificate authority
#   keys/server.pem  the TLS proxy's certificate and key, valid for "localhost" and
#                    "libsql.spelaion.test" but not for 127.0.0.1: the tests connect to 127.0.0.1
#                    and check the certificate against a name, as through a tunnel
#   keys/jwt.pub     the Ed25519 key the server with authentication checks tokens with
#   keys/token.jwt   a token it accepts, signed with keys/jwt.key (the connection's password)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p keys
# Git Bash on Windows rewrites arguments that start with "/" (like -subj "/CN=…").
export MSYS_NO_PATHCONV=1

if [ ! -f keys/ca.crt ]; then
  openssl req -x509 -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/ca.key -out keys/ca.crt -days 3650 -subj "/CN=SpelaionDB test CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
fi

if [ ! -f keys/server.pem ]; then
  openssl req -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/server.key -out keys/server.csr -subj "/CN=libsql.spelaion.test" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,DNS:libsql.spelaion.test\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n' > keys/server.ext
  openssl x509 -req -in keys/server.csr -CA keys/ca.crt -CAkey keys/ca.key -CAcreateserial \
    -out keys/server.crt -days 3650 -extfile keys/server.ext 2>/dev/null
  cat keys/server.crt keys/server.key > keys/server.pem
  rm -f keys/server.csr keys/server.ext keys/ca.srl
fi

# base64url without padding, as JWT writes it.
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

if [ ! -f keys/token.jwt ]; then
  openssl genpkey -algorithm ed25519 -out keys/jwt.key 2>/dev/null
  openssl pkey -in keys/jwt.key -pubout -out keys/jwt.pub 2>/dev/null
  header=$(printf '{"alg":"EdDSA","typ":"JWT"}' | b64url)
  payload=$(printf '{"a":"rw"}' | b64url)
  printf '%s.%s' "$header" "$payload" > keys/token.input
  signature=$(openssl pkeyutl -sign -rawin -inkey keys/jwt.key -in keys/token.input | b64url)
  printf '%s.%s.%s' "$header" "$payload" "$signature" > keys/token.jwt
  rm -f keys/token.input
fi

echo "Keys in $(pwd)/keys"
