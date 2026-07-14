#!/usr/bin/env bash

set -euo pipefail

# Windows does not currently provide the exact retained-parent namespace
# persistence primitive required by production central-column publication.
# Keep this list exact and small: windows-core still runs every other package,
# while collections retains a focused fail-closed capability test in the
# workflow.
readonly -a windows_namespace_unsupported_packages=(
  "github.com/snissn/gomap/TreeDB/collections"
  "github.com/snissn/gomap/TreeDB/documentservice"
)

if [ "$#" -ne 0 ]; then
  echo "usage: go list ./... | $0" >&2
  exit 2
fi

mapfile -t packages < <(sed -e 's/\r$//' -e '/^$/d')
if [ "${#packages[@]}" -eq 0 ]; then
  echo "TreeDB windows-core package routing received an empty package list" >&2
  exit 1
fi
if [ "${#windows_namespace_unsupported_packages[@]}" -ne 2 ]; then
  echo "TreeDB windows-core namespace capability boundary must contain exactly two packages" >&2
  exit 1
fi

for excluded in "${windows_namespace_unsupported_packages[@]}"; do
  occurrences=0
  for package in "${packages[@]}"; do
    if [ "$package" = "$excluded" ]; then
      occurrences=$((occurrences + 1))
    fi
  done
  if [ "$occurrences" -ne 1 ]; then
    echo "TreeDB windows-core expected exact package $excluded once, found $occurrences" >&2
    exit 1
  fi
  echo "TreeDB windows-core exclusion package=$excluded reason=namespace_persistence_unsupported" >&2
done

for package in "${packages[@]}"; do
  case "$package" in
    github.com/snissn/gomap/TreeDB | github.com/snissn/gomap/TreeDB/db | github.com/snissn/gomap/TreeDB/caching)
      # Root and DB tests are name-sharded below; caching has dedicated shards.
      ;;
    github.com/snissn/gomap/TreeDB/collections | github.com/snissn/gomap/TreeDB/documentservice)
      # Exact Windows capability exclusions validated above.
      ;;
    *)
      printf '%s\n' "$package"
      ;;
  esac
done
