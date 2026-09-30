#!/usr/bin/env bash
set -e

# Load predefined functions for preparing trust- and keystores
source ./tls_utils.sh

echo "Preparing truststore for replication listener"
# Add each certificate to the trust store
STORE=/tmp/kafka/cluster.truststore.p12
rm -f "$STORE"
for CRT in /opt/kafka/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for replication listener is complete"

echo "Looking for the CA matching the server certificate"
CA=$(find_ca /opt/kafka/cluster-ca-certs "/opt/kafka/broker-certs/$HOSTNAME.crt")

if [ ! -f "$CA" ]; then
    echo "No CA matching the server certificate found. This process will exit with failure."
    exit 1
fi
echo "CA matching the server certificate found: $CA"

echo "Preparing keystore for replication and clienttls listener"
STORE=/tmp/kafka/cluster.keystore.p12
rm -f "$STORE"
create_keystore "$STORE" "$CERTS_STORE_PASSWORD" \
    "/opt/kafka/broker-certs/$HOSTNAME.crt" \
    "/opt/kafka/broker-certs/$HOSTNAME.key" \
    "$CA" \
    "$HOSTNAME"
echo "Preparing keystore for replication and clienttls listener is complete"

# Handle both legacy and cluster-stretching listener identifier formats:
#   legacy:  /opt/kafka/certificates/(custom|oauth)-<name>-<port>-certs    (e.g. oauth-internal-9094-certs)
#   forked:  /opt/kafka/certificates/(custom|oauth)-<name>-certs           (e.g. oauth-internal-certs)
# The fork in ListenersUtils.identifier() drops the -port suffix from the
# directory name, so the original 3-group regex no longer matches and OAuth
# truststore preparation would silently be skipped. We try the legacy regex
# first (it's more specific), then fall back to the no-port form.
regex_with_port="^\/opt\/kafka\/certificates\/(custom|oauth)-(.+)-([0-9]+)-certs$"
regex_no_port="^\/opt\/kafka\/certificates\/(custom|oauth)-(.+)-certs$"
for CERT_DIR in /opt/kafka/certificates/*; do
  prefix=""
  name=""
  port=""
  listener=""
  if [[ $CERT_DIR =~ $regex_with_port ]]; then
    prefix=${BASH_REMATCH[1]}
    name=${BASH_REMATCH[2]}
    port=${BASH_REMATCH[3]}
    listener=${prefix}-${name}-${port}
  elif [[ $CERT_DIR =~ $regex_no_port ]]; then
    prefix=${BASH_REMATCH[1]}
    name=${BASH_REMATCH[2]}
    listener=${prefix}-${name}
  else
    continue
  fi

  echo "Preparing store for $listener listener"
  if [[ $prefix == "custom"  ]]; then
    echo "Creating keystore /tmp/kafka/$listener.keystore.p12"
    rm -f /tmp/kafka/"$listener".keystore.p12
    create_keystore_without_ca_file /tmp/kafka/"$listener".keystore.p12 "$CERTS_STORE_PASSWORD" "${CERT_DIR}/tls.crt" "${CERT_DIR}/tls.key" custom-key
  elif [[ $prefix == "oauth"  ]]; then
    if [[ -n "$port" ]]; then
      trusted_certs="STRIMZI_${name^^}_${port}_OAUTH_TRUSTED_CERTS"
    else
      trusted_certs="STRIMZI_${name^^}_OAUTH_TRUSTED_CERTS"
    fi
    if [ -n "${!trusted_certs}" ]; then
      prepare_truststore "/tmp/kafka/$listener.truststore.p12" "$CERTS_STORE_PASSWORD" "$CERT_DIR" "${!trusted_certs}"
    fi
  fi
  echo "Preparing store for $prefix $name listener is complete"
done

echo "Preparing truststore for client authentication"
# Add each certificate to the trust store
STORE=/tmp/kafka/clients.truststore.p12
rm -f "$STORE"
for CRT in /opt/kafka/client-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for client authentication is complete"

if [ -n "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Open Policy Agent authorization truststore"
  prepare_truststore "/tmp/kafka/authz-opa.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-opa-certs" "$STRIMZI_OPA_AUTHZ_TRUSTED_CERTS"
fi

if [ -n "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS" ]; then
  echo "Preparing Keycloak authorization truststore"
  prepare_truststore "/tmp/kafka/authz-keycloak.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/kafka/certificates/authz-keycloak-certs" "$STRIMZI_KEYCLOAK_AUTHZ_TRUSTED_CERTS"
fi
