#!/bin/bash

set -eo pipefail

PROJECT="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python3}

echo "$PYTHON_BIN"

set -x

rm -rf "$PROJECT"/dist
rm -rf "$PROJECT"/build
mkdir -p "$PROJECT"/dist

cd "$PROJECT"
$PYTHON_BIN setup.py sdist bdist_wheel

set +x
