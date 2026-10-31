#!/usr/bin/env bash
set -euo pipefail
evidence="$RUNNER_TEMP/5342-${ROLE:?role required}"
controls="$GITHUB_WORKSPACE/harness/.github/5342"
[[ "$GITHUB_SHA" == "$GITHUB_WORKFLOW_SHA" ]]
[[ "$(git rev-parse HEAD)" == 5e1b999069a0e92d2eabb89fef6e1be0fae1c634 ]]
[[ "$(git rev-parse HEAD^{tree})" == c8ee66558a348514043e53cbe0fa12d76d719eb1 ]]
[[ "$(git -C ../harness rev-parse HEAD)" == "$GITHUB_SHA" ]]
[[ -z "$(git status --porcelain --untracked-files=all)" ]]
git cat-file -e 0538bf2dc25519bbeabe968d35b025c240f4d4f7^{commit}
git rev-parse HEAD HEAD^{tree} > "$evidence/product.txt"
git -C ../harness rev-parse HEAD HEAD^{tree} > "$evidence/controller.txt"
cp -R "$controls" "$evidence/sources"
cp ../harness/.github/workflows/treedb-vacuum-m0-observation.yml "$evidence/carrier-workflow.yml"
mkdir -p "$evidence/product/.github/workflows" "$evidence/product/scripts"
cp go.mod go.sum "$evidence/product/"
cp .github/workflows/read-snapshot-guardrails.yml .github/workflows/unified-bench-tests.yml "$evidence/product/.github/workflows/"
cp scripts/observe_read_snapshot_cache.sh scripts/check_read_snapshot_guardrail.sh "$evidence/product/scripts/"
cmp scripts/observe_read_snapshot_cache.sh "$controls/candidate/scripts/observe_read_snapshot_cache.sh"
cmp .github/workflows/read-snapshot-guardrails.yml "$controls/candidate/.github/workflows/read-snapshot-guardrails.yml"
sha256sum scripts/check_read_snapshot_guardrail.sh scripts/observe_read_snapshot_cache.sh .github/workflows/read-snapshot-guardrails.yml .github/workflows/unified-bench-tests.yml go.mod go.sum > "$evidence/product-source-hashes.txt"
printf '%s\n' '21aaf7758b336619af3f761bc01a4033f82732363cabb8803173a233f06100d1  scripts/observe_read_snapshot_cache.sh' '1f2e65a20d688cda41ce5af4e5e7b482e5e2cdf1c4b69c15212e7e0b8027f5cd  scripts/check_read_snapshot_guardrail.sh' | sha256sum --check -
printf 'expected_key=%s\n' 'setup-go-Linux-x64-ubuntu24-go-1.26.10-aaf8042bc95790b8abf66d434e53939ea143ed6648a0f7450d39ec8f1ad097d4' > "$evidence/expected-key.txt"
git --version > "$evidence/tools.txt"
bash --version >> "$evidence/tools.txt"
python3 --version >> "$evidence/tools.txt"
