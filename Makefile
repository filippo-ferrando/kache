# Variables
GO := go
BINARY_DAEMON := dcdnd
BINARY_CLI := kachectl
SRC_DAEMON := ./cmd/dcdnd/main.go
SRC_CLI := ./cmd/kache/main.go

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

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning up binaries..."
	rm -f $(BINARY_DAEMON) $(BINARY_CLI)
