.PHONY: build test test-unit test-integration

# Default test target: unit tests only (no external dependencies).
test: test-unit

build:
	go build -o bin/task-graph .

test-unit:
	go vet ./...
	go test ./...

# REQUIREMENTS.md §7: run the complete integration-only tier with its own emulator.
test-integration:
	@./scripts/test-integration.sh
