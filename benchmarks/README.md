# Performance Benchmarks

This directory contains performance benchmarks for the Delta Lake implementation.
Benchmarks use [Vitest's built-in benchmarking](https://vitest.dev/guide/features.html#benchmarking) capabilities.

## Running Benchmarks

### Run All Benchmarks

```bash
npm run bench
```

### Run Specific Benchmark Suites

```bash
# Write throughput benchmarks
npm run bench:write

# Query latency benchmarks
npm run bench:query

# Compaction speed benchmarks
npm run bench:compaction
```

### Run with Filtering

```bash
# Run specific benchmark by name pattern
npm run bench -- -t "write 1000"

# Run with verbose output
npm run bench -- --reporter=verbose
```

## Benchmark Suites

### Write Throughput (`write.bench.ts`)

Measures write operation performance:

- **Batch Sizes**: Tests different batch sizes (10, 100, 1000, 10000 docs)
- **Sequential Writes**: Multiple sequential write operations
- **Document Complexity**: Flat vs nested vs large documents
- **Incremental Writes**: Appending to existing tables
- **Partitioned Tables**: Write performance with partitions

Key metrics:
- Documents per second
- Bytes per second
- Latency per batch

### Query Latency (`query.bench.ts`)

Measures query operation performance:

- **Full Table Scan**: Reading all documents from tables of various sizes
- **Equality Filters**: Single and multi-field equality conditions
- **Comparison Filters**: Range queries, $in, $exists operators
- **Logical Operators**: $and, $or, complex nested queries
- **Nested Field Access**: Dot-notation path queries
- **Projections**: Include/exclude field selection
- **Multi-File Tables**: Query performance across many Parquet files
- **Regex Matching**: Pattern-based string filtering

Key metrics:
- Documents per second
- Latency per query
- Filter selectivity impact

### Compaction Speed (`compaction.bench.ts`)

Measures optimization operation performance:

- **File Merging**: Combining fragmented small files
- **Compaction Strategies**: bin-packing, greedy, sort-by-size
- **Deduplication**: Primary key and exact duplicate removal
- **Z-Order Clustering**: Single and multi-column clustering
- **Hilbert Curves**: Alternative space-filling curve
- **Combined Operations**: Compact + deduplicate pipelines
- **Integrity Verification**: Overhead of data verification

Key metrics:
- Files processed per second
- Rows processed per second
- Compaction ratio

## Benchmark Results

Results are output to the console in verbose format and also saved to `benchmarks/results.json` for further analysis.

### Example Output

```
 BENCH  Summary

  Write Throughput - Batch Sizes > Medium batches (100 docs)
    name                hz     min     max    mean     p75     p99    p995   p999     rme  samples
  · write 100 docs   85.23  10.24   15.43   11.73   12.01   15.43   15.43   15.43  ±3.21%       10
```

Columns:
- `hz`: Operations per second
- `min/max/mean`: Latency statistics (ms)
- `p75/p99/p995/p999`: Latency percentiles (ms)
- `rme`: Relative margin of error (%)
- `samples`: Number of iterations

## Writing Custom Benchmarks

### Basic Structure

```typescript
import { bench, describe } from 'vitest'
import { createBenchmarkTable, generateDocuments } from './utils'

describe('My Benchmark Suite', () => {
  bench(
    'benchmark name',
    async () => {
      // Code to benchmark
      const { table } = createBenchmarkTable()
      await table.write(generateDocuments(100))
    },
    {
      iterations: 10,        // Number of measured iterations
      warmupIterations: 3,   // Warmup before measuring
      time: 60000,           // Timeout in ms
    }
  )
})
```

### Using Setup/Teardown

```typescript
import { bench, describe, beforeAll, afterAll } from 'vitest'

describe('Benchmark with Setup', () => {
  let table: DeltaTable

  beforeAll(async () => {
    // Setup runs once before all benchmarks
    const result = await createPopulatedTable(10000)
    table = result.table
  })

  afterAll(() => {
    // Cleanup after all benchmarks
    table.storage.clear()
  })

  bench('query on pre-populated table', async () => {
    await table.query({ category: 'electronics' })
  })
})
```

### Utility Functions

The `utils.ts` file provides helpers:

- `generateDocuments(count, startId?)` - Generate test documents
- `generateNestedDocuments(count)` - Generate nested structure documents
- `generateLargeDocument(sizeKb)` - Generate large payload documents
- `createBenchmarkTable()` - Create fresh table with MemoryStorage
- `createPopulatedTable(count, batchSize?)` - Create pre-filled table
- `createFragmentedTable(files, rowsPerFile)` - Create fragmented table

## Best Practices

1. **Isolate State**: Each benchmark iteration should start with fresh state
2. **Warmup**: Use warmup iterations to avoid cold-start effects
3. **Consistent Data**: Use the same data generators for comparable results
4. **Multiple Iterations**: Run enough iterations for statistical significance
5. **Document Baseline**: Record baseline results before optimization work

## Continuous Integration

Benchmarks can be run in CI to detect performance regressions:

```yaml
# Example GitHub Actions step
- name: Run benchmarks
  run: npm run bench
  continue-on-error: true  # Don't fail CI on benchmark results
```

For regression detection, compare `benchmarks/results.json` between runs.
