.PHONY: test coverage clean

# Default Go test flags
GOTEST_FLAGS := -v

# Default package path (all packages)
PKG_PATH := ./...

# Coverage output file
COVERAGE_OUT := coverage.out
COVERAGE_HTML := coverage.html

# Run tests
test:
	go test $(GOTEST_FLAGS) $(PKG_PATH)

# Run tests with coverage
coverage:
	go test $(GOTEST_FLAGS) -coverprofile=$(COVERAGE_OUT) $(PKG_PATH)
	go tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	go tool cover -func=$(COVERAGE_OUT)

# Run tests with coverage for a specific package
# Usage: make test-pkg PKG=./path/to/package
test-pkg:
	@if [ -z "$(PKG)" ]; then \
		echo "Error: PKG is not set. Usage: make test-pkg PKG=./path/to/package"; \
		exit 1; \
	fi
	go test $(GOTEST_FLAGS) $(PKG)

# Run coverage for a specific package
# Usage: make coverage-pkg PKG=./path/to/package
coverage-pkg:
	@if [ -z "$(PKG)" ]; then \
		echo "Error: PKG is not set. Usage: make coverage-pkg PKG=./path/to/package"; \
		exit 1; \
	fi
	go test $(GOTEST_FLAGS) -coverprofile=$(COVERAGE_OUT) $(PKG)
	go tool cover -func=$(COVERAGE_OUT)
