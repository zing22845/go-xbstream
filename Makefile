VET_FLAGS=-assign -atomic -bools -copylocks -errorsas -nilfunc -printf -stdmethods -unusedresult -unreachable -tests

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -X github.com/zing22845/go-xbstream/internal/version.Version=$(VERSION) \
           -X github.com/zing22845/go-xbstream/internal/version.GitCommit=$(COMMIT) \
           -X github.com/zing22845/go-xbstream/internal/version.BuildDate=$(BUILD_DATE)

all: build vet test

build:
	go build ./...
	go build -ldflags "$(LDFLAGS)" -o bin/xbstream ./cmd/xbstream
	go build -ldflags "$(LDFLAGS)" -o bin/xbcrypt ./cmd/xbcrypt
	go build -ldflags "$(LDFLAGS)" -o bin/try_extract ./cmd/try_extract

test:
	go test ./pkg/...
	go test ./internal/...

vet:
	go vet ${VET_FLAGS} ./...

clean:
	rm -rf bin/

install:
	go install -ldflags "$(LDFLAGS)" ./cmd/xbstream
	go install -ldflags "$(LDFLAGS)" ./cmd/xbcrypt

.PHONY: build vet test all clean install