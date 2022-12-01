#!/bin/bash

set -e

mkdir -p output

NYDDUSIY_SUBDIR=contrib/nydusify

make -C ${NYDDUSIY_SUBDIR} release
install -D -m 755 ${NYDDUSIY_SUBDIR}/cmd/nydusify output
