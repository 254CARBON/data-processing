#!/bin/bash
# Generate CA certificates for internal service communication

set -e

CERT_DIR="certs"
CA_KEY="$CERT_DIR/ca-key.pem"
CA_CERT="$CERT_DIR/ca-cert.pem"
CA_CONFIG="$CERT_DIR/ca.conf"

# Create certs directory
mkdir -p "$CERT_DIR"

# Generate CA private key
echo "Generating CA private key..."
openssl genrsa -out "$CA_KEY" 4096

# Create CA configuration
cat > "$CA_CONFIG" << EOF
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_ca
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = San Francisco
O = 254Carbon
OU = Data Processing
CN = 254Carbon CA

[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
EOF

# Generate CA certificate
echo "Generating CA certificate..."
openssl req -new -x509 -days 3650 -key "$CA_KEY" -out "$CA_CERT" -config "$CA_CONFIG"

# Generate service certificates
services=("normalization-service" "enrichment-service" "aggregation-service" "projection-service")

for service in "${services[@]}"; do
    echo "Generating certificate for $service..."
    
    # Service private key
    openssl genrsa -out "$CERT_DIR/${service}-key.pem" 2048
    
    # Service certificate request
    openssl req -new -key "$CERT_DIR/${service}-key.pem" -out "$CERT_DIR/${service}.csr" -subj "/C=US/ST=CA/L=San Francisco/O=254Carbon/OU=Data Processing/CN=$service"
    
    # Service certificate
    openssl x509 -req -in "$CERT_DIR/${service}.csr" -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$CERT_DIR/${service}-cert.pem" -days 365 -extensions v3_req -extfile <(
        cat << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = San Francisco
O = 254Carbon
OU = Data Processing
CN = $service

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = $service
DNS.2 = $service.default.svc.cluster.local
DNS.3 = localhost
IP.1 = 127.0.0.1
EOF
    )
    
    # Clean up CSR
    rm "$CERT_DIR/${service}.csr"
done

# Generate client certificates for service-to-service communication
echo "Generating client certificates..."
openssl genrsa -out "$CERT_DIR/client-key.pem" 2048
openssl req -new -key "$CERT_DIR/client-key.pem" -out "$CERT_DIR/client.csr" -subj "/C=US/ST=CA/L=San Francisco/O=254Carbon/OU=Data Processing/CN=client"
openssl x509 -req -in "$CERT_DIR/client.csr" -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial -out "$CERT_DIR/client-cert.pem" -days 365 -extensions v3_req -extfile <(
    cat << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = CA
L = San Francisco
O = 254Carbon
OU = Data Processing
CN = client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF
)

# Clean up client CSR
rm "$CERT_DIR/client.csr"

# Set proper permissions
chmod 600 "$CERT_DIR"/*.pem
chmod 644 "$CERT_DIR"/*.pem

echo "Certificate generation complete!"
echo "CA Certificate: $CA_CERT"
echo "CA Key: $CA_KEY"
echo "Service certificates generated for: ${services[*]}"
echo "Client certificate: $CERT_DIR/client-cert.pem"
