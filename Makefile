# raft-kv — build & test entry points
#
# Work milestone by milestone:
#   make test-m1   # leader election
#   make test-m2   # log replication + KV
#   make test-m3   # persistence / crash recovery
#   make test-m4   # snapshots & log compaction
#   make test      # everything
#
# RACE DETECTOR: all test targets run with -race. Raft is concurrent by
# nature; the race detector will catch locking mistakes the tests can't.

GO      ?= go
PKGS    := ./...
TESTFLAGS := -race -timeout 120s

.PHONY: check build vet fmt test test-m1 test-m2 test-m3 test-m4 test-harness cover clean

check: fmt vet build ## format check, vet, and compile everything

build:
	$(GO) build $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

test:
	$(GO) test $(TESTFLAGS) $(PKGS)

test-m1:
	$(GO) test $(TESTFLAGS) ./raft/ -run 'TestM1' -v

test-m2:
	$(GO) test $(TESTFLAGS) ./raft/ ./kv/ -run 'TestM2' -v

test-m3:
	$(GO) test $(TESTFLAGS) ./raft/ -run 'TestM3' -v

test-m4:
	$(GO) test $(TESTFLAGS) ./raft/ -run 'TestM4' -v

test-harness: ## sanity-check the plumbing itself (should pass from day one)
	$(GO) test $(TESTFLAGS) ./transport/ ./storage/ -v

cover:
	$(GO) test $(TESTFLAGS) -coverprofile=coverage.txt $(PKGS) || true
	$(GO) tool cover -html=coverage.txt -o coverage.html
	@echo "open coverage.html"

clean:
	rm -f coverage.txt coverage.html
	rm -rf bin data
