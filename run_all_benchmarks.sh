#!/bin/bash
# Run benchmarks for all environments

set -e  # Exit on error

ENVIRONMENTS=(
  "py12-dtp-stable-postgres13"
  "py12-dtp-pr355-postgres13"
  "py12-dtp-offsets-postgres13"
  "py12-dtp-hybrid-postgres13"
)

for env in "${ENVIRONMENTS[@]}"; do
  echo ""
  echo "========================================"
  echo "Running benchmark for: $env"
  echo "========================================"

  if pixi run -e "$env" benchmark; then
    echo "✓ Benchmark completed for $env"
  else
    echo "✗ Benchmark failed for $env"
    exit 1
  fi

  # Small delay between runs
  sleep 2
done

echo ""
echo "========================================"
echo "All benchmarks completed!"
echo "========================================"
