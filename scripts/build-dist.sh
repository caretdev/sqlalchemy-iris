#!/bin/bash

packages=("iris" "intersystems_iris" "irisnative")
for package in ${packages[@]};
do
    rm -f ./$package
    package_path=`python -c "import importlib.util; print(importlib.util.find_spec('${package}').submodule_search_locations[0])"`
    ln -s $package_path ./$package
done

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

for package in ${packages[@]};
do
    rm -f $package
done
rm -rf intersystems-irispython

set +x
