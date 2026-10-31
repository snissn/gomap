#!/usr/bin/env bash
# Metadata only: do not download modules or reveal cache paths or environment.
set -u
case "${SETUP_GO_CACHE_HIT-}" in
  true|false) printf '[cache-observation] setup_go_cache_hit=%s\n' "$SETUP_GO_CACHE_HIT" ;;
  *) printf '[cache-observation] setup_go_cache_hit=unknown\n' ;;
esac
if ! module_cache="$(go env GOMODCACHE 2>/dev/null)" || [[ -z "$module_cache" ]]; then
  printf '[cache-observation] module_cache=unavailable\n'
  exit 0
fi
module_dir="$module_cache/github.com/antlabs/stl@v0.0.2"
download_dir="$module_cache/cache/download/github.com/antlabs/stl/@v"
if [[ -L "$module_dir" ]]; then
  printf '[cache-observation] extracted_module_dir=symlink\n'
elif [[ -d "$module_dir" ]]; then
  printf '[cache-observation] extracted_module_dir=present\n'
else
  printf '[cache-observation] extracted_module_dir=absent\n'
fi
for suffix in zip ziphash; do
  file="$download_dir/v0.0.2.$suffix"
  if [[ -L "$file" ]]; then
    printf '[cache-observation] %s=symlink\n' "$suffix"
  elif [[ -f "$file" ]] && size="$(stat -c %s -- "$file" 2>/dev/null)"; then
    printf '[cache-observation] %s=present bytes=%s\n' "$suffix" "$size"
  elif [[ -e "$file" ]]; then
    printf '[cache-observation] %s=unreadable_or_not_regular\n' "$suffix"
  else
    printf '[cache-observation] %s=absent\n' "$suffix"
  fi
done
exit 0
