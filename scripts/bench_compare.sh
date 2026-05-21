#!/usr/bin/env bash
# Run the crawler benchmark suite on the current branch and a comparison branch.
# Usage: ./scripts/bench_compare.sh [branch]   (default: main)
set -euo pipefail

COMPARE="${1:-main}"
BENCHES=(bench_sparse_io bench_burst_routing bench_crawler_topology bench_idle_latency)

run_suite() {
    local dir="$1" label="$2"
    echo ""
    echo "── $label ──"
    (cd "$dir" && cargo build --release --examples -q)
    for b in "${BENCHES[@]}"; do
        (cd "$dir" && cargo run --example "$b" --release 2>/dev/null) | grep "BENCH_JSON:"
    done
}

WORKTREE=$(mktemp -d)
trap "git worktree remove '$WORKTREE' --force 2>/dev/null || true" EXIT

git worktree add "$WORKTREE" "$COMPARE" -q
cp examples/bench_*.rs "$WORKTREE/examples/"

run_suite "." "$(git branch --show-current)"
run_suite "$WORKTREE" "$COMPARE"
