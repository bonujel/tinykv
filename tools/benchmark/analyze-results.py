#!/usr/bin/env python3
"""
TinyKV Benchmark Results Analyzer

Parses JSON benchmark results and generates:
- Markdown report with comparison tables
- CSV files for graphing
- Performance summary statistics
"""

import json
import sys
import csv
from pathlib import Path
from typing import List, Dict, Any


def load_results(json_path: str) -> List[Dict[str, Any]]:
    """Load benchmark results from JSON file."""
    with open(json_path, 'r') as f:
        return json.load(f)


def format_latency(us: int) -> str:
    """Format latency in microseconds to human-readable string."""
    if us < 1000:
        return f"{us}µs"
    elif us < 1000000:
        return f"{us/1000:.2f}ms"
    else:
        return f"{us/1000000:.2f}s"


def generate_markdown_report(results: List[Dict[str, Any]], output_path: str):
    """Generate markdown report with comparison tables."""
    with open(output_path, 'w') as f:
        f.write("# TinyKV Benchmark Results\n\n")
        f.write(f"Total runs: {len(results)}\n\n")

        # Summary table
        f.write("## Performance Summary\n\n")
        f.write("| Workload | Records | Throughput (ops/s) | P50 | P95 | P99 | Errors |\n")
        f.write("|----------|---------|-------------------|-----|-----|-----|--------|\n")

        for r in results:
            workload = r.get('workload', r.get('label', 'unknown'))
            records = r['records']
            throughput = r['run_ops_per_sec']
            p50 = format_latency(r.get('run_p50_us', 0))
            p95 = format_latency(r.get('run_p95_us', 0))
            p99 = format_latency(r.get('run_p99_us', 0))
            errors = r['load_errors'] + r['run_errors']

            f.write(f"| {workload} | {records:,} | {throughput:,.0f} | {p50} | {p95} | {p99} | {errors} |\n")

        # Detailed results
        f.write("\n## Detailed Results\n\n")
        for i, r in enumerate(results, 1):
            workload = r.get('workload', r.get('label', 'unknown'))
            f.write(f"### Run {i}: {workload}\n\n")

            f.write("**Configuration:**\n")
            f.write(f"- Records: {r['records']:,}\n")
            f.write(f"- Operations: {r['ops']:,}\n")
            f.write(f"- Value size: {r['value_size']} bytes\n")
            f.write(f"- Threads: {r['threads']}\n\n")

            f.write("**Load Phase:**\n")
            f.write(f"- Throughput: {r['load_ops_per_sec']:,.0f} ops/s\n")
            f.write(f"- Duration: {r['load_elapsed_ms']:,}ms\n")
            f.write(f"- Data written: {r['load_write_mb']:.2f} MB\n")
            f.write(f"- Errors: {r['load_errors']}\n\n")

            f.write("**Run Phase:**\n")
            f.write(f"- Throughput: {r['run_ops_per_sec']:,.0f} ops/s\n")
            f.write(f"- Duration: {r['run_elapsed_ms']:,}ms\n")
            f.write(f"- Avg latency: {format_latency(r.get('run_avg_us', 0))}\n")
            f.write(f"- P50 latency: {format_latency(r.get('run_p50_us', 0))}\n")
            f.write(f"- P95 latency: {format_latency(r.get('run_p95_us', 0))}\n")
            f.write(f"- P99 latency: {format_latency(r.get('run_p99_us', 0))}\n")
            f.write(f"- Max latency: {format_latency(r.get('run_max_us', 0))}\n")
            f.write(f"- Errors: {r['run_errors']}\n\n")

            # Chaos results if present
            if r.get('chaos_enabled'):
                f.write("**Fault Injection (Chaos):**\n")
                f.write(f"- Target: {r.get('chaos_target', 'N/A')}\n")
                f.write(f"- Injected at: {r.get('chaos_injected_at_ms', 0)}ms\n")
                f.write(f"- Error window: {r.get('error_window_ms', 0)}ms\n")
                f.write(f"- Recovery time (RTO): {r.get('recovery_ms', 0)}ms\n")
                f.write(f"- Errors during RTO: {r.get('errors_during_rto', 0)}\n\n")

    print(f"Markdown report written to {output_path}")


def generate_csv(results: List[Dict[str, Any]], output_path: str):
    """Generate CSV file for graphing."""
    with open(output_path, 'w', newline='') as f:
        fieldnames = [
            'workload', 'records', 'ops', 'value_size', 'threads',
            'load_ops_per_sec', 'load_errors', 'load_elapsed_ms',
            'run_ops_per_sec', 'run_errors', 'run_elapsed_ms',
            'run_avg_us', 'run_p50_us', 'run_p95_us', 'run_p99_us', 'run_max_us'
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for r in results:
            # Normalize workload field
            row = r.copy()
            if 'label' in row and 'workload' not in row:
                row['workload'] = row['label']
            writer.writerow(row)

    print(f"CSV data written to {output_path}")


def print_summary(results: List[Dict[str, Any]]):
    """Print summary statistics to console."""
    print("\n" + "="*60)
    print("BENCHMARK SUMMARY")
    print("="*60)

    total_ops = sum(r['ops'] for r in results)
    avg_throughput = sum(r['run_ops_per_sec'] for r in results) / len(results)
    total_errors = sum(r['load_errors'] + r['run_errors'] for r in results)

    print(f"Total runs:        {len(results)}")
    print(f"Total operations:  {total_ops:,}")
    print(f"Avg throughput:    {avg_throughput:,.0f} ops/s")
    print(f"Total errors:      {total_errors}")

    # Find best/worst performers
    best = max(results, key=lambda r: r['run_ops_per_sec'])
    worst = min(results, key=lambda r: r['run_ops_per_sec'])

    print(f"\nBest performer:    {best.get('workload', best.get('label'))} "
          f"({best['run_ops_per_sec']:,.0f} ops/s)")
    print(f"Worst performer:   {worst.get('workload', worst.get('label'))} "
          f"({worst['run_ops_per_sec']:,.0f} ops/s)")

    # Latency statistics
    avg_p50 = sum(r.get('run_p50_us', 0) for r in results) / len(results)
    avg_p99 = sum(r.get('run_p99_us', 0) for r in results) / len(results)

    print(f"\nAvg P50 latency:   {format_latency(int(avg_p50))}")
    print(f"Avg P99 latency:   {format_latency(int(avg_p99))}")
    print("="*60 + "\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze-results.py <results.json> [output_prefix]")
        print("\nGenerates:")
        print("  - <output_prefix>_report.md  (markdown report)")
        print("  - <output_prefix>_data.csv   (CSV for graphing)")
        sys.exit(1)

    json_path = sys.argv[1]
    output_prefix = sys.argv[2] if len(sys.argv) > 2 else "benchmark"

    # Load results
    try:
        results = load_results(json_path)
    except FileNotFoundError:
        print(f"Error: File not found: {json_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}")
        sys.exit(1)

    if not results:
        print("Error: No results found in JSON file")
        sys.exit(1)

    # Generate outputs
    md_path = f"{output_prefix}_report.md"
    csv_path = f"{output_prefix}_data.csv"

    generate_markdown_report(results, md_path)
    generate_csv(results, csv_path)
    print_summary(results)

    print(f"\nAnalysis complete!")
    print(f"  Report: {md_path}")
    print(f"  Data:   {csv_path}")


if __name__ == "__main__":
    main()
