#!/usr/bin/env bash
# Builds loadgen/loadgen. No dependencies beyond gcc.
set -euo pipefail
cd "$(dirname "$0")"
CC=${CC:-cc}
$CC -O2 -Wall -Wextra -Wno-unused-parameter -o loadgen loadgen.c -lpthread
echo "built $(pwd)/loadgen"
