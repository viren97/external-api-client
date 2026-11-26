#!/usr/bin/env bash
# Generates a throwaway CA, a server cert (localhost) and a client cert for the TLS/mTLS tests.
# Output: src/test/resources/certs. Password for every store: changeit. Never use these outside tests.
set -euo pipefail
OUT="$(cd "$(dirname "$0")/.." && pwd)/src/test/resources/certs"
rm -rf "$OUT" && mkdir -p "$OUT" && cd "$OUT"
P=changeit
kt() { keytool -noprompt "$@" >/dev/null 2>&1; }

kt -genkeypair -alias ca -keyalg RSA -keysize 2048 -dname "CN=ext-api Test CA" -validity 3650 \
   -ext bc:c -keystore ca.p12 -storetype PKCS12 -storepass $P
kt -exportcert -rfc -alias ca -keystore ca.p12 -storepass $P -file ca.pem

issue() { # name cn san
  kt -genkeypair -alias "$1" -keyalg RSA -keysize 2048 -dname "CN=$2" -validity 3650 -keystore "$1.p12" -storetype PKCS12 -storepass $P
  keytool -certreq -alias "$1" -keystore "$1.p12" -storepass $P -file "$1.csr"
  keytool -gencert -alias ca -keystore ca.p12 -storepass $P -infile "$1.csr" -outfile "$1.crt" -rfc -validity 3650 ${3:+-ext "SAN=$3"} \
    -ext KU=digitalSignature,keyEncipherment
  kt -importcert -alias ca -file ca.pem -keystore "$1.p12" -storepass $P
  cat "$1.crt" ca.pem > "$1-chain.pem"
  kt -importcert -alias "$1" -file "$1-chain.pem" -keystore "$1.p12" -storepass $P
  rm "$1.csr"
}
issue server localhost "dns:localhost,ip:127.0.0.1"
issue client ext-api-test-client ""

# Trust store with only the CA.
kt -importcert -alias ca -file ca.pem -keystore truststore.p12 -storetype PKCS12 -storepass $P
# PEM client key (PKCS#8) for the PEM-based mTLS test.
openssl pkcs12 -in client.p12 -nocerts -nodes -passin pass:$P 2>/dev/null | openssl pkcs8 -topk8 -nocrypt -out client-key.pem
# A second, unrelated CA to prove untrusted chains are rejected.
kt -genkeypair -alias other -keyalg RSA -keysize 2048 -dname "CN=Other CA" -validity 3650 -keystore other.p12 -storetype PKCS12 -storepass $P
kt -exportcert -rfc -alias other -keystore other.p12 -storepass $P -file other-ca.pem
rm -f other.p12 ca.p12
echo "certs written to $OUT"; ls "$OUT"
