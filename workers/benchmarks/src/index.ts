/**
 * Benchmark Worker - Unified API for MongoLake, KafkaLake, and ParqueDB
 *
 * Uses ClickBench dataset schema for realistic benchmarking:
 * - MongoLake: MongoDB-compatible API
 * - KafkaLake: Kafka-style CDC producer/consumer
 * - ParqueDB: Direct Parquet read/write
 *
 * @see https://github.com/ClickHouse/ClickBench
 */

import {
  DeltaTable,
  createCDCDeltaTable,
  CDCProducer,
  MemoryOffsetStorage,
  type Filter,
  type CDCDeltaTable,
  aggregate,
} from '../../../src/index.js'
import { R2Storage } from '../../../src/storage/index.js'

interface Env {
  BUCKET: R2Bucket
  ENVIRONMENT: string
}

// =============================================================================
// CLICKBENCH SCHEMA - Web Analytics Data
// =============================================================================

/**
 * ClickBench hits table schema (simplified for benchmarking)
 * Full schema has 105 columns - we use the most relevant ones
 */
interface ClickBenchHit {
  WatchID: string
  JavaEnable: number
  Title: string
  GoodEvent: number
  EventTime: string
  EventDate: string
  CounterID: number
  ClientIP: number
  RegionID: number
  UserID: string
  CounterClass: number
  OS: number
  UserAgent: number
  URL: string
  Referer: string
  IsRefresh: number
  RefererCategoryID: number
  RefererRegionID: number
  URLCategoryID: number
  URLRegionID: number
  ResolutionWidth: number
  ResolutionHeight: number
  ResolutionDepth: number
  FlashMajor: number
  FlashMinor: number
  NetMajor: number
  NetMinor: number
  UserAgentMajor: number
  CookieEnable: number
  JavascriptEnable: number
  IsMobile: number
  MobilePhone: number
  MobilePhoneModel: string
  Params: string
  IPNetworkID: number
  TraficSourceID: number
  SearchEngineID: number
  SearchPhrase: string
  AdvEngineID: number
  IsArtifical: number
  WindowClientWidth: number
  WindowClientHeight: number
  ClientTimeZone: number
  Age: number
  Sex: number
  Income: number
  CodeVersion: number
  ClickDate: string
  ClickGoodEvent: number
  ClickPriority: number
  ClickHitID: string
}

/**
 * Generate realistic ClickBench-style data
 */
function generateClickBenchHit(index: number): ClickBenchHit {
  const now = new Date()
  const eventTime = new Date(now.getTime() - Math.random() * 86400000 * 30) // Last 30 days

  return {
    WatchID: `${Date.now()}${index}${Math.random().toString(36).slice(2, 10)}`,
    JavaEnable: Math.random() > 0.1 ? 1 : 0,
    Title: `Page Title ${index % 1000}`,
    GoodEvent: Math.random() > 0.05 ? 1 : 0,
    EventTime: eventTime.toISOString(),
    EventDate: eventTime.toISOString().split('T')[0],
    CounterID: Math.floor(Math.random() * 10000),
    ClientIP: Math.floor(Math.random() * 4294967295),
    RegionID: Math.floor(Math.random() * 300),
    UserID: `user-${Math.floor(Math.random() * 100000)}`,
    CounterClass: Math.floor(Math.random() * 10),
    OS: Math.floor(Math.random() * 20),
    UserAgent: Math.floor(Math.random() * 100),
    URL: `https://example.com/page/${index % 10000}?ref=${Math.random().toString(36).slice(2, 8)}`,
    Referer: Math.random() > 0.3 ? `https://google.com/search?q=${Math.random().toString(36).slice(2, 8)}` : '',
    IsRefresh: Math.random() > 0.9 ? 1 : 0,
    RefererCategoryID: Math.floor(Math.random() * 20),
    RefererRegionID: Math.floor(Math.random() * 300),
    URLCategoryID: Math.floor(Math.random() * 50),
    URLRegionID: Math.floor(Math.random() * 300),
    ResolutionWidth: [1920, 1366, 1440, 1536, 2560][Math.floor(Math.random() * 5)],
    ResolutionHeight: [1080, 768, 900, 864, 1440][Math.floor(Math.random() * 5)],
    ResolutionDepth: 24,
    FlashMajor: 0,
    FlashMinor: 0,
    NetMajor: 0,
    NetMinor: 0,
    UserAgentMajor: Math.floor(Math.random() * 120),
    CookieEnable: Math.random() > 0.05 ? 1 : 0,
    JavascriptEnable: Math.random() > 0.02 ? 1 : 0,
    IsMobile: Math.random() > 0.6 ? 1 : 0,
    MobilePhone: Math.floor(Math.random() * 100),
    MobilePhoneModel: '',
    Params: '',
    IPNetworkID: Math.floor(Math.random() * 10000),
    TraficSourceID: Math.floor(Math.random() * 10),
    SearchEngineID: Math.floor(Math.random() * 30),
    SearchPhrase: Math.random() > 0.7 ? `search term ${Math.floor(Math.random() * 1000)}` : '',
    AdvEngineID: Math.floor(Math.random() * 30),
    IsArtifical: 0,
    WindowClientWidth: [1920, 1366, 1440][Math.floor(Math.random() * 3)],
    WindowClientHeight: [969, 657, 789][Math.floor(Math.random() * 3)],
    ClientTimeZone: Math.floor(Math.random() * 24) - 12,
    Age: Math.floor(Math.random() * 80) + 18,
    Sex: Math.floor(Math.random() * 3),
    Income: Math.floor(Math.random() * 10),
    CodeVersion: Math.floor(Math.random() * 1000),
    ClickDate: eventTime.toISOString().split('T')[0],
    ClickGoodEvent: Math.random() > 0.1 ? 1 : 0,
    ClickPriority: Math.floor(Math.random() * 10),
    ClickHitID: `${Date.now()}${index}`,
  }
}

// =============================================================================
// STORAGE
// =============================================================================

// Prefix for all benchmark data in the shared cdn bucket
const BENCHMARK_PREFIX = 'deltalake-benchmarks'

function createR2Storage(bucket: R2Bucket): R2Storage {
  return new R2Storage({ bucket, prefix: BENCHMARK_PREFIX })
}

// =============================================================================
// SHARED STATE
// =============================================================================

const offsetStorage = new MemoryOffsetStorage()
const producers = new Map<string, CDCProducer<ClickBenchHit>>()
const cdcTables = new Map<string, CDCDeltaTable<ClickBenchHit>>()

// =============================================================================
// MONGOLAKE API
// =============================================================================

async function handleMongoLake(
  request: Request,
  storage: R2Storage,
  path: string[]
): Promise<Response> {
  const [, , database, collection, operation] = path
  const tablePath = `mongolake/${database}/${collection}`
  const table = new DeltaTable<ClickBenchHit>(storage, tablePath)

  const start = performance.now()

  try {
    switch (operation) {
      case 'insertOne': {
        const body = await request.json() as { document: ClickBenchHit }
        const doc = { WatchID: crypto.randomUUID(), ...body.document }
        await table.write([doc])
        return jsonResponse({
          acknowledged: true,
          insertedId: doc.WatchID,
          latencyMs: performance.now() - start,
        })
      }

      case 'insertMany': {
        const body = await request.json() as { documents: ClickBenchHit[] }
        await table.write(body.documents)
        return jsonResponse({
          acknowledged: true,
          insertedCount: body.documents.length,
          latencyMs: performance.now() - start,
        })
      }

      case 'find': {
        const body = await request.json() as { filter?: Filter<ClickBenchHit>, limit?: number }
        const results = await table.query(body.filter)
        const limited = body.limit ? results.slice(0, body.limit) : results
        return jsonResponse({
          documents: limited,
          count: limited.length,
          latencyMs: performance.now() - start,
        })
      }

      case 'aggregate': {
        const body = await request.json() as { pipeline: Parameters<typeof aggregate>[1] }
        const allDocs = await table.query()
        const result = aggregate(allDocs, body.pipeline)
        return jsonResponse({
          documents: result.documents,
          stats: result.stats,
          latencyMs: performance.now() - start,
        })
      }

      case 'count': {
        const body = await request.json() as { filter?: Filter<ClickBenchHit> }
        const results = await table.query(body.filter)
        return jsonResponse({
          count: results.length,
          latencyMs: performance.now() - start,
        })
      }

      default:
        return jsonResponse({ error: `Unknown operation: ${operation}` }, 400)
    }
  } catch (error) {
    return jsonResponse({
      error: error instanceof Error ? error.message : 'Unknown error',
      latencyMs: performance.now() - start,
    }, 500)
  }
}

// =============================================================================
// KAFKALAKE API
// =============================================================================

async function handleKafkaLake(
  request: Request,
  storage: R2Storage,
  path: string[]
): Promise<Response> {
  const start = performance.now()

  try {
    // POST /kafkalake/topics/{topic}/produce
    if (path[2] === 'topics' && path[4] === 'produce') {
      const topic = path[3]
      let producer = producers.get(topic)
      if (!producer) {
        producer = new CDCProducer<ClickBenchHit>({ source: { database: 'kafkalake', collection: topic } })
        producers.set(topic, producer)
      }

      const body = await request.json() as { records: ClickBenchHit[] }
      const tablePath = `kafkalake/${topic}`
      let cdcTable = cdcTables.get(tablePath)
      if (!cdcTable) {
        cdcTable = createCDCDeltaTable<ClickBenchHit>(storage, tablePath)
        await cdcTable.setCDCEnabled(true)
        cdcTables.set(tablePath, cdcTable)
      }

      // Produce records
      const cdcRecords = body.records.map(record =>
        producer!.create(record.WatchID, record)
      )

      // Write to CDC table
      await cdcTable.write(body.records)

      return jsonResponse({
        produced: cdcRecords.length,
        latencyMs: performance.now() - start,
      })
    }

    // GET /kafkalake/topics/{topic}/consume
    if (path[2] === 'topics' && path[4] === 'consume') {
      const topic = path[3]
      const tablePath = `kafkalake/${topic}`
      let cdcTable = cdcTables.get(tablePath)
      if (!cdcTable) {
        cdcTable = createCDCDeltaTable<ClickBenchHit>(storage, tablePath)
        cdcTables.set(tablePath, cdcTable)
      }

      const url = new URL(request.url)
      const fromVersion = BigInt(url.searchParams.get('fromVersion') ?? '0')
      const limit = parseInt(url.searchParams.get('limit') ?? '1000')

      const reader = cdcTable.getCDCReader()
      const changes = await reader.readByVersion(fromVersion, fromVersion + BigInt(limit))

      return jsonResponse({
        records: changes,
        count: changes.length,
        latencyMs: performance.now() - start,
      })
    }

    // POST /kafkalake/topics/{topic}/commit
    if (path[2] === 'topics' && path[4] === 'commit') {
      const topic = path[3]
      const body = await request.json() as { groupId: string, offset: string }

      await offsetStorage.commitOffset(body.groupId, topic, {
        offset: BigInt(body.offset),
        committedAt: new Date(),
      })

      return jsonResponse({
        committed: true,
        latencyMs: performance.now() - start,
      })
    }

    return jsonResponse({ error: 'Unknown KafkaLake endpoint' }, 404)
  } catch (error) {
    return jsonResponse({
      error: error instanceof Error ? error.message : 'Unknown error',
      latencyMs: performance.now() - start,
    }, 500)
  }
}

// =============================================================================
// PARQUEDB API
// =============================================================================

async function handleParqueDB(
  request: Request,
  storage: R2Storage,
  path: string[]
): Promise<Response> {
  const start = performance.now()

  try {
    // POST /parquedb/tables/{table}/write
    if (path[2] === 'tables' && path[4] === 'write') {
      const tableName = path[3]
      const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
      const body = await request.json() as { rows: ClickBenchHit[] }

      const commit = await table.write(body.rows)

      return jsonResponse({
        version: commit.version,
        rowsWritten: body.rows.length,
        latencyMs: performance.now() - start,
      })
    }

    // POST /parquedb/tables/{table}/query
    if (path[2] === 'tables' && path[4] === 'query') {
      const tableName = path[3]
      const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
      const body = await request.json() as { filter?: Filter<ClickBenchHit>, limit?: number }

      const results = await table.query(body.filter)
      const limited = body.limit ? results.slice(0, body.limit) : results

      return jsonResponse({
        rows: limited,
        count: limited.length,
        latencyMs: performance.now() - start,
      })
    }

    // GET /parquedb/tables/{table}/version
    if (path[2] === 'tables' && path[4] === 'version') {
      const tableName = path[3]
      const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
      const version = await table.version()

      return jsonResponse({
        version,
        latencyMs: performance.now() - start,
      })
    }

    return jsonResponse({ error: 'Unknown ParqueDB endpoint' }, 404)
  } catch (error) {
    return jsonResponse({
      error: error instanceof Error ? error.message : 'Unknown error',
      latencyMs: performance.now() - start,
    }, 500)
  }
}

// =============================================================================
// BENCHMARK ENDPOINTS - ClickBench Workloads
// =============================================================================

interface BenchmarkResult {
  implementation: string
  operation: string
  recordCount: number
  latencyMs: number
  throughputPerSec: number
}

async function handleBenchmark(
  request: Request,
  storage: R2Storage,
  path: string[]
): Promise<Response> {
  const start = performance.now()
  const url = new URL(request.url)

  // GET /benchmark/ingest/{implementation}?count=1000&batchSize=100
  if (path[1] === 'ingest') {
    const implementation = path[2] as 'mongolake' | 'kafkalake' | 'parquedb'
    const count = parseInt(url.searchParams.get('count') ?? '1000')
    const batchSize = parseInt(url.searchParams.get('batchSize') ?? '100')
    const tableName = `clickbench-${Date.now()}`

    const results: BenchmarkResult[] = []
    let totalRecords = 0

    for (let i = 0; i < count; i += batchSize) {
      const batch = Array.from(
        { length: Math.min(batchSize, count - i) },
        (_, j) => generateClickBenchHit(i + j)
      )

      const batchStart = performance.now()

      if (implementation === 'mongolake') {
        const table = new DeltaTable<ClickBenchHit>(storage, `mongolake/benchmark/${tableName}`)
        await table.write(batch)
      } else if (implementation === 'kafkalake') {
        const cdcTable = createCDCDeltaTable<ClickBenchHit>(storage, `kafkalake/${tableName}`)
        if (i === 0) await cdcTable.setCDCEnabled(true)
        await cdcTable.write(batch)
      } else {
        const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
        await table.write(batch)
      }

      const batchLatency = performance.now() - batchStart
      totalRecords += batch.length

      results.push({
        implementation,
        operation: 'ingest',
        recordCount: batch.length,
        latencyMs: batchLatency,
        throughputPerSec: (batch.length / batchLatency) * 1000,
      })
    }

    const totalLatency = performance.now() - start

    return jsonResponse({
      implementation,
      tableName,
      totalRecords,
      batchSize,
      batches: results.length,
      totalLatencyMs: totalLatency,
      avgBatchLatencyMs: totalLatency / results.length,
      overallThroughputPerSec: (totalRecords / totalLatency) * 1000,
      results,
    })
  }

  // GET /benchmark/query/{implementation}?table=xxx
  if (path[1] === 'query') {
    const implementation = path[2] as 'mongolake' | 'kafkalake' | 'parquedb'
    const tableName = url.searchParams.get('table')
    if (!tableName) {
      return jsonResponse({ error: 'table parameter required' }, 400)
    }

    const queries: Array<{ name: string, filter: Filter<ClickBenchHit> }> = [
      { name: 'full_scan', filter: {} },
      { name: 'by_region', filter: { RegionID: 100 } },
      { name: 'mobile_only', filter: { IsMobile: 1 } },
      { name: 'good_events', filter: { GoodEvent: 1 } },
      { name: 'date_range', filter: { EventDate: { $gte: '2024-01-01' } } },
    ]

    const results: Array<{ query: string, rowCount: number, latencyMs: number }> = []

    for (const q of queries) {
      const queryStart = performance.now()
      let rowCount = 0

      if (implementation === 'mongolake') {
        const table = new DeltaTable<ClickBenchHit>(storage, `mongolake/benchmark/${tableName}`)
        const rows = await table.query(q.filter)
        rowCount = rows.length
      } else if (implementation === 'kafkalake') {
        const cdcTable = createCDCDeltaTable<ClickBenchHit>(storage, `kafkalake/${tableName}`)
        const rows = await cdcTable.query(q.filter)
        rowCount = rows.length
      } else {
        const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
        const rows = await table.query(q.filter)
        rowCount = rows.length
      }

      results.push({
        query: q.name,
        rowCount,
        latencyMs: performance.now() - queryStart,
      })
    }

    return jsonResponse({
      implementation,
      tableName,
      queries: results,
      totalLatencyMs: performance.now() - start,
    })
  }

  // GET /benchmark/compare/ingest?count=1000&batchSize=100
  if (path[1] === 'compare' && path[2] === 'ingest') {
    const count = parseInt(url.searchParams.get('count') ?? '1000')
    const batchSize = parseInt(url.searchParams.get('batchSize') ?? '100')

    const implementations = ['mongolake', 'kafkalake', 'parquedb'] as const
    const comparison: Record<string, { totalLatencyMs: number, throughputPerSec: number }> = {}

    for (const impl of implementations) {
      const tableName = `compare-${impl}-${Date.now()}`
      const implStart = performance.now()

      for (let i = 0; i < count; i += batchSize) {
        const batch = Array.from(
          { length: Math.min(batchSize, count - i) },
          (_, j) => generateClickBenchHit(i + j)
        )

        if (impl === 'mongolake') {
          const table = new DeltaTable<ClickBenchHit>(storage, `mongolake/benchmark/${tableName}`)
          await table.write(batch)
        } else if (impl === 'kafkalake') {
          const cdcTable = createCDCDeltaTable<ClickBenchHit>(storage, `kafkalake/${tableName}`)
          if (i === 0) await cdcTable.setCDCEnabled(true)
          await cdcTable.write(batch)
        } else {
          const table = new DeltaTable<ClickBenchHit>(storage, `parquedb/${tableName}`)
          await table.write(batch)
        }
      }

      const implLatency = performance.now() - implStart
      comparison[impl] = {
        totalLatencyMs: implLatency,
        throughputPerSec: (count / implLatency) * 1000,
      }
    }

    return jsonResponse({
      recordCount: count,
      batchSize,
      comparison,
      totalLatencyMs: performance.now() - start,
    })
  }

  return jsonResponse({
    endpoints: {
      '/benchmark/ingest/{implementation}': 'Test ingest performance (mongolake, kafkalake, parquedb)',
      '/benchmark/query/{implementation}': 'Test query performance',
      '/benchmark/compare/ingest': 'Compare all implementations side-by-side',
    },
  })
}

// =============================================================================
// MAIN HANDLER
// =============================================================================

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(
    JSON.stringify(data, (_, v) => typeof v === 'bigint' ? v.toString() : v, 2),
    {
      status,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    }
  )
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const path = url.pathname.split('/').filter(Boolean)
    const storage = createR2Storage(env.BUCKET)

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      })
    }

    // Health check
    if (path[0] === 'health') {
      return jsonResponse({
        status: 'ok',
        environment: env.ENVIRONMENT,
        timestamp: new Date().toISOString(),
      })
    }

    // Route to implementation
    switch (path[0]) {
      case 'mongolake':
        return handleMongoLake(request, storage, path)
      case 'kafkalake':
        return handleKafkaLake(request, storage, path)
      case 'parquedb':
        return handleParqueDB(request, storage, path)
      case 'benchmark':
        return handleBenchmark(request, storage, path)
      default:
        return jsonResponse({
          name: 'DeltaLake Benchmark Worker',
          version: '0.0.1',
          dataset: 'ClickBench (web analytics)',
          implementations: ['mongolake', 'kafkalake', 'parquedb'],
          endpoints: {
            mongolake: 'POST /mongolake/{db}/{collection}/{insertOne|insertMany|find|aggregate|count}',
            kafkalake: 'POST /kafkalake/topics/{topic}/{produce|consume|commit}',
            parquedb: 'POST /parquedb/tables/{table}/{write|query}',
            benchmark: 'GET /benchmark/{ingest|query|compare}/{implementation}',
          },
        })
    }
  },
}
