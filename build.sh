#!/bin/bash

set -e

make release

mkdir -p output

install -D -m 755 target/release/nydusd output
install -D -m 755 target/release/nydus-image output
install -D -m 755 target/release/nydusctl output

NYDDUSIY_SUBDIR=contrib/nydusify

make -C ${NYDDUSIY_SUBDIR} release
install -D -m 755 ${NYDDUSIY_SUBDIR}/cmd/nydusify output
