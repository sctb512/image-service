#!/bin/bash

set -e

make release

mkdir -p output

install -D -m 755 target/release/nydusd output
install -D -m 755 target/release/nydus-image output
install -D -m 755 target/release/nydusctl output
