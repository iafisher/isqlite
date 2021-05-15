#!/bin/bash

set -eu

main() {
  if [[ $# -lt 1 ]]; then
    usage
  fi

  subcommand="$1"
  if [[ "$subcommand" = test ]]; then
    shift
    .venv/bin/python3 -m unittest "$@"
  elif [[ "$subcommand" = publish ]]; then
    if [[ $# -lt 2 ]]; then
      usage
    fi
    version="$2"
    # Remove leading 'v' from version, if present.
    version="${version%v}"

    # Courtesy of https://unix.stackexchange.com/questions/13466/
    current_version=$(grep -oP 'version="\K[0-9.]+' setup.py)

    if [[ "$version" = "$current_version" ]]; then
      echo "New version is same as current version. Aborting."
      exit 1
    fi

    changed_files="$(git diff --name-only) $(git diff --cached --name-only)"
    # Trim whitespace, courtesy of https://stackoverflow.com/questions/369758/
    changed_files=$(echo "$changed_files" | xargs)
    if [[ -n "$changed_files" ]]; then
      echo "Files changed in repository:"
      echo
      git diff --name-only
      git diff --cached --name-only
      echo
      echo "Aborting."
      exit 1
    fi

    read -p "Bump version to $version? (currently: $current_version) " -r
    if [[ ! $REPLY =~ ^[Yy] ]]; then
      echo "Aborted."
      exit 1
    fi

    echo
    echo
    echo "=== Replacing the version number in setup.py ==="
    sed -i "s/version=\".*\"/version=\"$version\"/" setup.py
    git add setup.py
    git commit -m "Bump to version $version"

    echo
    echo
    echo "=== Tagging the current commit in git ==="
    git tag -a "v$version" -m "Version $version"

    echo
    echo
    echo "=== Pushing changes to the remote repository ==="
    git push origin master
    git push --tags

    # https://packaging.python.org/tutorials/packaging-projects/
    echo
    echo
    echo "=== Packaging the project ==="
    rm -f dist/*
    python3 setup.py sdist bdist_wheel

    echo
    echo
    echo "=== Uploading to PyPI ==="
    twine upload dist/*
  else
    usage
  fi
}

usage() {
  echo "Usage:"
  echo "  $0 test"
  echo "  $0 publish <version>"
  exit 1
}

main "$@"
