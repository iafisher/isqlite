#!/bin/bash

set -eu

main() {
  if [[ $# -lt 1 ]]; then
    usage
  fi

  subcommand="$1"
  shift
  if [[ "$subcommand" = test ]]; then
    python3 -m unittest "$@"
  elif [[ "$subcommand" = publish ]]; then
    # https://packaging.python.org/tutorials/packaging-projects/
    rm -f dist/*
    python3 setup.py sdist bdist_wheel
    twine upload dist/*
  else
    usage
  fi
}

usage() {
  echo "Expected subcommand: test or publish"
  exit 1
}

main "$@"
