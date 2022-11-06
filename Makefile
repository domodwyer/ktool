SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

# Set the default goal if "make" is run without arguments
.DEFAULT_GOAL := help

# Delete any targets that failed to force them to be rebuilt on the next run
.DELETE_ON_ERROR:

# Disable built-in C/Lex/Yacc rules, and warn if referencing undefined vars
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

######################################################

VPATH = src
SRC = $(wildcard *.rs)
VERSION = $(shell cat Cargo.toml | sed -n 's/^version = "\(.*\)"$$/\1/p')

target/aarch64-apple-darwin/release/ktool: $(SRC)
	cargo build --release --target aarch64-apple-darwin

target/x86_64-unknown-linux-musl/release/ktool: $(SRC)
	CC_x86_64_unknown_linux_musl=x86_64-unknown-linux-gnu-gcc \
	CXX_x86_64_unknown_linux_musl=x86_64-unknown-linux-gnu-g++ \
	AR_x86_64_unknown_linux_musl=x86_64-unknown-linux-gnu-ar \
	CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc \
	CC=x86_64-unknown-linux-musl-gcc \
	CXX=x86_64-unknown-linux-gnu-g++ \
	LD=x86_64-unknown-linux-musl-ld \
		cargo build --release --target x86_64-unknown-linux-musl

release/ktool_v$(VERSION)_%: target/%/release/ktool
	@-mkdir -p $(dir $@)
	cp $^ $@

%.sha256sum: %
	sha256sum "$^" > $@

%.tar.gz: %
	tar cv "$^" | gzip --best > "$@" 

%.zip: %
	zip "$@" $^

#? release: generate release binaries for Linux and macOS
.PHONY: release
release: release/ktool_v$(VERSION)_x86_64-unknown-linux-musl.tar.gz \
	release/ktool_v$(VERSION)_x86_64-unknown-linux-musl.tar.gz.sha256sum \
	release/ktool_v$(VERSION)_aarch64-apple-darwin.zip \
	release/ktool_v$(VERSION)_aarch64-apple-darwin.zip.sha256sum

#? clean: remove any generated files
.PHONY: clean
clean:
	-rm -rf release
	cargo clean

#? help: prints this help message
.PHONY: help
help:
	@echo "Usage:"
	@sed -n 's/^#?//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'