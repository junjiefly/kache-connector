GO ?= go
PYTHON ?= python3
BUILD_DIR := build

.PHONY: all proto build shlib shlib-windows fmt go-test test smoke clean

all: build

proto:
	$(PYTHON) tools/generate_proto.py

build:
	$(GO) build ./...
	$(GO) build -o $(BUILD_DIR)/kvc-smoke ./cmd/kvc-smoke

shlib:
	$(GO) build -buildmode=c-shared -o $(BUILD_DIR)/libkvcache.so ./cmd/kvc-shlib

shlib-windows:
	$(GO) build -buildmode=c-shared -o $(BUILD_DIR)/kvcache.dll ./cmd/kvc-shlib

fmt:
	gofmt -w .

go-test:
	$(GO) test ./...

# The public package tests the connector core and Transport SPI. Production
# data-plane adapters are supplied by a separate private integration build.
test: fmt go-test

smoke: build
	$(BUILD_DIR)/kvc-smoke

clean:
	rm -rf $(BUILD_DIR)
