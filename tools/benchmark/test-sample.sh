#!/bin/bash
# Sample test script for TinyKV benchmark tool
# This script demonstrates all major features

set -e

BENCHMARK="./benchmark"
ANALYZE="./analyze-results.py"

echo "=========================================="
echo "TinyKV Benchmark Tool - Sample Tests"
echo "=========================================="
echo ""

# Check if benchmark binary exists
if [ ! -f "$BENCHMARK" ]; then
    echo "Building benchmark tool..."
    go build -o benchmark
    echo "Build complete!"
    echo ""
fi

# Make analysis script executable
chmod +x "$ANALYZE"

# Test 1: Single YCSB Workload
echo "Test 1: Running YCSB Workload B (Read-heavy)"
echo "Command: $BENCHMARK -workload-preset B -records 1000 -ops 1000 -threads 4"
echo ""
$BENCHMARK -workload-preset B -records 1000 -ops 1000 -threads 4
echo ""
echo "✓ Test 1 complete"
echo ""

# Test 2: YCSB Suite
echo "Test 2: Running YCSB Suite (All workloads A-F)"
echo "Command: $BENCHMARK -ycsb-suite -records 1000 -ops 1000 -threads 4 -json-out ycsb_test.json"
echo ""
$BENCHMARK -ycsb-suite -records 1000 -ops 1000 -threads 4 -json-out ycsb_test.json
echo ""
echo "✓ Test 2 complete"
echo ""

# Test 3: Analysis
echo "Test 3: Analyzing results"
echo "Command: $ANALYZE ycsb_test.json ycsb_test"
echo ""
$ANALYZE ycsb_test.json ycsb_test
echo ""
echo "✓ Test 3 complete"
echo ""

# Test 4: Scale Suite
echo "Test 4: Running Scale Suite (10K/100K/1M)"
echo "Command: $BENCHMARK -suite -workload-preset B -threads 4 -json-out scale_test.json"
echo ""
echo "Note: This will take several minutes..."
# Uncomment to run full scale test
# $BENCHMARK -suite -workload-preset B -threads 4 -json-out scale_test.json
echo "Skipped (uncomment in script to run)"
echo ""

# Test 5: Custom Mixed Workload
echo "Test 5: Custom mixed workload (80% read, 20% write)"
echo "Command: $BENCHMARK -workload mixed -read-ratio 0.8 -records 1000 -ops 1000 -threads 4"
echo ""
$BENCHMARK -workload mixed -read-ratio 0.8 -records 1000 -ops 1000 -threads 4
echo ""
echo "✓ Test 5 complete"
echo ""

# Summary
echo "=========================================="
echo "All tests complete!"
echo "=========================================="
echo ""
echo "Generated files:"
echo "  - ycsb_test.json         (Raw benchmark results)"
echo "  - ycsb_test_report.md    (Markdown report)"
echo "  - ycsb_test_data.csv     (CSV for graphing)"
echo ""
echo "Next steps:"
echo "  1. Review ycsb_test_report.md for detailed results"
echo "  2. Import ycsb_test_data.csv into Excel/Google Sheets"
echo "  3. Run full scale test (uncomment Test 4 in script)"
echo "  4. Try chaos testing: $BENCHMARK -workload-preset B -records 10000 -ops 10000 -chaos"
echo ""
echo "For resume/interview preparation, see QUICKSTART.md"
echo ""
