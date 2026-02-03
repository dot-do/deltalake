# @dotdo/deltalake - Claude Code Context

## Project Purpose

Shared Delta Lake foundation for MongoLake (MongoDB API) and KafkaLake (Kafka API).
Extracts common components from MongoLake into a reusable package.

## Key Patterns

### Storage Backend Interface
```typescript
interface StorageBackend {
  read(path: string): Promise<Uint8Array>
  write(path: string, data: Uint8Array): Promise<void>
  list(prefix: string): Promise<string[]>
  delete(path: string): Promise<void>
  exists(path: string): Promise<boolean>
}
```

### Delta Log Format (JSON in _delta_log/)
```json
{"add": {"path": "part-00000.parquet", "size": 1234, "dataChange": true}}
{"commitInfo": {"timestamp": 1234567890, "operation": "WRITE"}}
```

### CDC Record Shape
```typescript
{ _id, _seq, _op: 'c'|'u'|'d'|'r', _before, _after, _ts, _source }
```

## Test Structure

- **Unit**: `tests/unit/` - vitest-pool-workers (miniflare)
- **Integration**: `tests/integration/` - multi-worker miniflare
- **E2E**: `tests/e2e/` - client against deltalake.dev API

## Extracting from MongoLake

Key files to port:
- `mongolake/src/storage/` → `deltalake/src/storage/`
- `mongolake/src/parquet/` → `deltalake/src/parquet/`
- `mongolake/src/types.ts` (Filter types) → `deltalake/src/query/`

## Dependencies

- `hyparquet` - Pure JS Parquet read/write
- No external lakehouse dependencies - we implement Delta format directly
