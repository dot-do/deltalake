# DeltaLake Benchmark Worker

Unified benchmark worker for testing MongoLake, KafkaLake, and ParqueDB implementations on Cloudflare Workers with R2 storage.

## Dataset

Uses [ClickBench](https://github.com/ClickHouse/ClickBench) web analytics schema for realistic benchmarking with 50+ columns of realistic web event data.

## Quick Start

```bash
# Install dependencies
npm install

# Run locally (requires wrangler login)
npm run dev

# Deploy to Cloudflare
npm run deploy
```

## API Endpoints

### MongoLake (MongoDB-compatible)

```bash
# Insert documents
curl -X POST http://localhost:8787/mongolake/mydb/events/insertMany \
  -H "Content-Type: application/json" \
  -d '{"documents": [{"Title": "Page 1", "RegionID": 100}]}'

# Find documents
curl -X POST http://localhost:8787/mongolake/mydb/events/find \
  -H "Content-Type: application/json" \
  -d '{"filter": {"RegionID": 100}}'

# Aggregate
curl -X POST http://localhost:8787/mongolake/mydb/events/aggregate \
  -H "Content-Type: application/json" \
  -d '{"pipeline": [{"$group": {"_id": "$RegionID", "count": {"$count": {}}}}]}'
```

### KafkaLake (Kafka-style CDC)

```bash
# Produce records
curl -X POST http://localhost:8787/kafkalake/topics/events/produce \
  -H "Content-Type: application/json" \
  -d '{"records": [{"WatchID": "123", "Title": "Event 1"}]}'

# Consume changes
curl http://localhost:8787/kafkalake/topics/events/consume?fromVersion=0&limit=100

# Commit offset
curl -X POST http://localhost:8787/kafkalake/topics/events/commit \
  -H "Content-Type: application/json" \
  -d '{"groupId": "my-group", "offset": "100"}'
```

### ParqueDB (Direct Parquet)

```bash
# Write rows
curl -X POST http://localhost:8787/parquedb/tables/events/write \
  -H "Content-Type: application/json" \
  -d '{"rows": [{"WatchID": "123", "Title": "Row 1"}]}'

# Query
curl -X POST http://localhost:8787/parquedb/tables/events/query \
  -H "Content-Type: application/json" \
  -d '{"filter": {"RegionID": 100}, "limit": 10}'

# Get version
curl http://localhost:8787/parquedb/tables/events/version
```

### Benchmarks

```bash
# Benchmark single implementation
curl "http://localhost:8787/benchmark/ingest/mongolake?count=1000&batchSize=100"
curl "http://localhost:8787/benchmark/ingest/kafkalake?count=1000&batchSize=100"
curl "http://localhost:8787/benchmark/ingest/parquedb?count=1000&batchSize=100"

# Compare all implementations
curl "http://localhost:8787/benchmark/compare/ingest?count=1000&batchSize=100"

# Query benchmark (after ingest)
curl "http://localhost:8787/benchmark/query/mongolake?table=clickbench-xxx"
```

## Running E2E Benchmark Tests

```bash
# Start the worker locally
cd workers/benchmarks
npm run dev

# In another terminal, run the E2E tests
cd ../..
npm run test:e2e -- tests/e2e/benchmarks.test.ts

# Or against production
BENCHMARK_URL=https://deltalake-benchmarks.your-account.workers.dev npm run test:e2e -- tests/e2e/benchmarks.test.ts
```

## Configuration

The worker uses `wrangler.jsonc` with:
- `compatibility_date`: 2026-01-01
- `nodejs_compat` flag enabled
- R2 bucket binding for storage

## Expected Performance

Real-world performance on Workers + R2 will be significantly different from local benchmarks:

| Metric | Local (Memory) | Workers + R2 |
|--------|----------------|--------------|
| Write latency | ~0.1ms | 50-200ms |
| Read latency | ~0.1ms | 20-100ms |
| Throughput | 8000+ ops/s | 10-50 ops/s |

The E2E tests measure **actual deployed performance** which is what matters for production.
