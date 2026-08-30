LOCAL_BIN := $(CURDIR)/bin
BUF_BUILD := $(LOCAL_BIN)/buf
GOLANGCI_LINT:=$(LOCAL_BIN)/golangci-lint

export GOBIN := $(LOCAL_BIN)

bin-deps:
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest


golint:
	$(GOLANGCI_LINT) run ./...
	
buf-generate:
	$(BUF_BUILD) generate

generate: buf-generate


tidy:
	go mod tidy

test:
	go test -v -race -timeout 60s ./...


cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

.PHONY: bin-deps buf-generate generate tidy