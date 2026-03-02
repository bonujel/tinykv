#!/usr/bin/env bash
# Quick demo of enhanced benchmark tool
set -euo pipefail

echo "=== TinyKV Enhanced Benchmark Demo ==="
echo ""

# Test 1: Single YCSB workload
echo "1. Running YCSB Workload B (95% read, 5% update)..."
./benchmark -workload-preset B -records 5000 -ops 5000 -threads 4 -progress 0

echo ""
echo "2. Running YCSB Workload C (100% read)..."
./benchmark -workload-preset C -records 5000 -ops 5000 -threads 4 -progress 0

echo ""
echo "3. Running YCSB Suite (all workloads A-F)..."
./benchmark -ycsb-suite -records 5000 -ops 5000 -threads 4 -progress 0 -json-out ycsb-results.json

echo ""
echo "=== Demo Complete ==="
echo "Results saved to: ycsb-results.json"
echo ""
echo "Next steps:"
echo "  - Run with larger datasets: -records 100000"
echo "  - Enable chaos testing: -chaos"
echo "  - Analyze results: python3 analyze-results.py ycsb-results.json"
