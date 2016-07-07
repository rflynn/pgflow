#!/bin/bash

set -x
set -e
set -o pipefail

NCPU=$(sysctl -n hw.ncpu || nproc || grep -c ^processor /proc/cpuinfo || echo 1)

echo setup virtualenv...
[ -d venv ] || { virtualenv -p python3 venv 2>/dev/null || python3 -m venv venv; }
venv/bin/pip3 install -r requirements.txt

echo build libpg_query...
mkdir -p vendor && cd vendor
[ -d libpg_query ] || git clone https://github.com/lfittl/libpg_query.git
cd libpg_query/
make -j $NCPU
