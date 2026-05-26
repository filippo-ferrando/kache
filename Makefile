# Variables
GO := go
BINARY_DAEMON := dcdnd
BINARY_CLI := kachectl
SRC_DAEMON := ./cmd/dcdnd/main.go
SRC_CLI := ./cmd/kache/main.go

# Node configuration for test certificate generation
# Can be overridden on execution: make gen-node NODE_NUM=2
NODE_NUM ?= 1

# Default target
.PHONY: all
all: build

# Build both binaries
.PHONY: build
build: build-daemon build-cli

# Build the dCDN daemon binary
.PHONY: build-daemon
build-daemon:
	@echo "Building dCDN daemon..."
	$(GO) build -o $(BINARY_DAEMON) $(SRC_DAEMON)

# Build the CLI tool binary
.PHONY: build-cli
build-cli:
	@echo "Building CLI tool..."
	$(GO) build -o $(BINARY_CLI) $(SRC_CLI)

# Run tests across all packages
.PHONY: test
test:
	@echo "Running tests..."
	$(GO) test -v ./...

# Format the Go source code
.PHONY: fmt
fmt:
	@echo "Formatting source code..."
	$(GO) fmt ./...

# Run go vet to examine source code for suspicious constructs
.PHONY: vet
vet:
	@echo "Vetting source code..."
	$(GO) vet ./...

# Generate Cluster-wide Root Certificate Authority credentials
.PHONY: gen-ca
gen-ca:
	@echo "Generating Cluster Root CA..."
	openssl genrsa -out cluster-ca.key 4096
	openssl req -x509 -new -nodes -key cluster-ca.key -sha256 -days 3650 \
		-out cluster-ca.crt \
		-subj "/CN=Kache Private Swarm Root CA/O=Kache CDN Cluster"

# Generate a parameterized Node certificate and private identity key
.PHONY: gen-node
gen-node:
	@if [ ! -f cluster-ca.key ] || [ ! -f cluster-ca.crt ]; then \
		echo "[Error] Root CA certificates missing. Run 'make gen-ca' first."; \
		exit 1; \
	fi
	@echo "Generating credentials for node number: $(NODE_NUM)..."
	@echo "authorityKeyIdentifier=keyid,issuer" > .node_ext.cnf
	@echo "basicConstraints=CA:FALSE" >> .node_ext.cnf
	@echo "keyUsage = digitalSignature, keyEncipherment" >> .node_ext.cnf
	@echo "extendedKeyUsage = clientAuth, serverAuth" >> .node_ext.cnf
	openssl genrsa -out node$(NODE_NUM).key 2048
	openssl req -new -key node$(NODE_NUM).key -out node$(NODE_NUM).csr \
		-subj "/CN=node$(NODE_NUM).kache.mesh/O=Kache Swarm Authorized Node"
	openssl x509 -req -in node$(NODE_NUM).csr -CA cluster-ca.crt -CAkey cluster-ca.key \
		-CAcreateserial -out node$(NODE_NUM).crt -days 365 -sha256 -extfile .node_ext.cnf
	@rm -f .node_ext.cnf

# Clean build artifacts and local generated test certificates
.PHONY: clean
clean:
	@echo "Cleaning up binaries and temporary cryptographic files..."
	rm -f $(BINARY_DAEMON) $(BINARY_CLI)
	rm -f *.key *.crt *.csr *.srl .node_ext.cnf
