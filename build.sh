#!/bin/bash

set -e

BINARIES_DIR="target/x86_64-unknown-linux-musl/release"

rustup target add x86_64-unknown-linux-musl
$(which sudo) apt install -y musl-dev

make static-release

mkdir -p output

install -D -m 755 ${BINARIES_DIR}/nydusd output
install -D -m 755 ${BINARIES_DIR}/nydus-image output
install -D -m 755 ${BINARIES_DIR}/nydusctl output
