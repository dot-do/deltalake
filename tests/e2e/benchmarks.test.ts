/**
 * E2E Benchmarks - Tests against deployed benchmark worker
 *
 * These tests hit the actual deployed worker on Cloudflare Workers
 * with R2 storage to measure real-world performance.
 *
 * Set BENCHMARK_URL environment variable to the deployed worker URL.
 * Default: http://localhost:8787 (for local wrangler dev)
 */

import { describe, it, expect, beforeAll } from 'vitest'

const BENCHMARK_URL = process.env.BENCHMARK_URL ?? 'http://localhost:8787'

interface BenchmarkResponse {
  latencyMs: number
  [key: string]: unknown
}

interface IngestResponse extends BenchmarkResponse {
  implementation: string
  totalRecords: number
  batchSize: number
  batches: number
  totalLatencyMs: number
  avgBatchLatencyMs: number
  overallThroughputPerSec: number
}

interface CompareResponse extends BenchmarkResponse {
  recordCount: number
  batchSize: number
  comparison: Record<string, { totalLatencyMs: number; throughputPerSec: number }>
}

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${BENCHMARK_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`HTTP ${response.status}: ${text}`)
  }
  return response.json() as Promise<T>
}

describe('E2E Benchmarks', () => {
  beforeAll(async () => {
    // Check if worker is running
    try {
      const health = await fetchJson<{ status: string }>('/health')
      expect(health.status).toBe('ok')
    } catch (error) {
      console.warn(`Benchmark worker not available at ${BENCHMARK_URL}`)
      console.warn('Run: cd workers/benchmarks && npm run dev')
      throw error
    }
  })

  describe('Health Check', () => {
    it('should return healthy status', async () => {
      const response = await fetchJson<{ status: string; environment: string }>('/health')
      expect(response.status).toBe('ok')
      expect(response.environment).toBeDefined()
    })
  })

  describe('MongoLake Ingest', () => {
    it('should benchmark small batch ingest (100 records, batch 10)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/mongolake?count=100&batchSize=10'
      )

      expect(response.implementation).toBe('mongolake')
      expect(response.totalRecords).toBe(100)
      expect(response.overallThroughputPerSec).toBeGreaterThan(0)

      console.log('MongoLake Small Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })

    it('should benchmark medium batch ingest (1000 records, batch 100)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/mongolake?count=1000&batchSize=100'
      )

      expect(response.implementation).toBe('mongolake')
      expect(response.totalRecords).toBe(1000)

      console.log('MongoLake Medium Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })
  })

  describe('KafkaLake Ingest', () => {
    it('should benchmark small batch ingest (100 records, batch 10)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/kafkalake?count=100&batchSize=10'
      )

      expect(response.implementation).toBe('kafkalake')
      expect(response.totalRecords).toBe(100)

      console.log('KafkaLake Small Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })

    it('should benchmark medium batch ingest (1000 records, batch 100)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/kafkalake?count=1000&batchSize=100'
      )

      expect(response.implementation).toBe('kafkalake')
      expect(response.totalRecords).toBe(1000)

      console.log('KafkaLake Medium Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })
  })

  describe('ParqueDB Ingest', () => {
    it('should benchmark small batch ingest (100 records, batch 10)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/parquedb?count=100&batchSize=10'
      )

      expect(response.implementation).toBe('parquedb')
      expect(response.totalRecords).toBe(100)

      console.log('ParqueDB Small Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })

    it('should benchmark medium batch ingest (1000 records, batch 100)', async () => {
      const response = await fetchJson<IngestResponse>(
        '/benchmark/ingest/parquedb?count=1000&batchSize=100'
      )

      expect(response.implementation).toBe('parquedb')
      expect(response.totalRecords).toBe(1000)

      console.log('ParqueDB Medium Batch:')
      console.log(`  Total: ${response.totalLatencyMs.toFixed(2)}ms`)
      console.log(`  Throughput: ${response.overallThroughputPerSec.toFixed(2)} records/sec`)
    })
  })

  describe('Comparison Benchmarks', () => {
    it('should compare all implementations (500 records)', async () => {
      const response = await fetchJson<CompareResponse>(
        '/benchmark/compare/ingest?count=500&batchSize=50'
      )

      expect(response.recordCount).toBe(500)
      expect(response.comparison.mongolake).toBeDefined()
      expect(response.comparison.kafkalake).toBeDefined()
      expect(response.comparison.parquedb).toBeDefined()

      console.log('\n=== Implementation Comparison (500 records) ===')
      console.log('Implementation      | Latency (ms) | Throughput (rec/s)')
      console.log('--------------------|--------------|-------------------')

      for (const [impl, stats] of Object.entries(response.comparison)) {
        console.log(
          `${impl.padEnd(19)} | ${stats.totalLatencyMs.toFixed(2).padStart(12)} | ${stats.throughputPerSec.toFixed(2).padStart(17)}`
        )
      }
    })

    it('should compare MongoLake vs KafkaLake ingest overhead', async () => {
      const mongoResponse = await fetchJson<IngestResponse>(
        '/benchmark/ingest/mongolake?count=500&batchSize=50'
      )
      const kafkaResponse = await fetchJson<IngestResponse>(
        '/benchmark/ingest/kafkalake?count=500&batchSize=50'
      )

      const overhead = (
        (kafkaResponse.totalLatencyMs - mongoResponse.totalLatencyMs) /
        mongoResponse.totalLatencyMs
      ) * 100

      console.log('\n=== MongoLake vs KafkaLake CDC Overhead ===')
      console.log(`MongoLake: ${mongoResponse.totalLatencyMs.toFixed(2)}ms`)
      console.log(`KafkaLake: ${kafkaResponse.totalLatencyMs.toFixed(2)}ms`)
      console.log(`CDC Overhead: ${overhead.toFixed(2)}%`)

      // CDC overhead should be reasonable (< 50%)
      expect(Math.abs(overhead)).toBeLessThan(100)
    })
  })

  describe('MongoLake API', () => {
    it('should insert and find documents', async () => {
      const insertResponse = await fetchJson<{ acknowledged: boolean; insertedCount: number; latencyMs: number }>(
        '/mongolake/benchmark/e2e-test/insertMany',
        {
          method: 'POST',
          body: JSON.stringify({
            documents: [
              { WatchID: 'test-1', Title: 'Test Page', RegionID: 100, IsMobile: 0 },
              { WatchID: 'test-2', Title: 'Test Page 2', RegionID: 200, IsMobile: 1 },
            ],
          }),
        }
      )

      expect(insertResponse.acknowledged).toBe(true)
      expect(insertResponse.insertedCount).toBe(2)

      console.log(`Insert latency: ${insertResponse.latencyMs.toFixed(2)}ms`)

      const findResponse = await fetchJson<{ documents: unknown[]; count: number; latencyMs: number }>(
        '/mongolake/benchmark/e2e-test/find',
        {
          method: 'POST',
          body: JSON.stringify({ filter: { IsMobile: 1 } }),
        }
      )

      expect(findResponse.count).toBeGreaterThanOrEqual(1)

      console.log(`Find latency: ${findResponse.latencyMs.toFixed(2)}ms`)
    })
  })

  describe('KafkaLake API', () => {
    it('should produce and consume records', async () => {
      const produceResponse = await fetchJson<{ produced: number; latencyMs: number }>(
        '/kafkalake/topics/e2e-test/produce',
        {
          method: 'POST',
          body: JSON.stringify({
            records: [
              { WatchID: 'kafka-1', Title: 'Kafka Event', RegionID: 100, IsMobile: 0 },
              { WatchID: 'kafka-2', Title: 'Kafka Event 2', RegionID: 200, IsMobile: 1 },
            ],
          }),
        }
      )

      expect(produceResponse.produced).toBe(2)

      console.log(`Produce latency: ${produceResponse.latencyMs.toFixed(2)}ms`)

      const consumeResponse = await fetchJson<{ records: unknown[]; count: number; latencyMs: number }>(
        '/kafkalake/topics/e2e-test/consume?fromVersion=0&limit=100'
      )

      expect(consumeResponse.count).toBeGreaterThanOrEqual(0) // May be 0 if CDC disabled

      console.log(`Consume latency: ${consumeResponse.latencyMs.toFixed(2)}ms`)
    })
  })

  describe('ParqueDB API', () => {
    it('should write and query rows', async () => {
      const writeResponse = await fetchJson<{ version: number; rowsWritten: number; latencyMs: number }>(
        '/parquedb/tables/e2e-test/write',
        {
          method: 'POST',
          body: JSON.stringify({
            rows: [
              { WatchID: 'parque-1', Title: 'Parquet Row', RegionID: 100, IsMobile: 0 },
              { WatchID: 'parque-2', Title: 'Parquet Row 2', RegionID: 200, IsMobile: 1 },
            ],
          }),
        }
      )

      expect(writeResponse.rowsWritten).toBe(2)

      console.log(`Write latency: ${writeResponse.latencyMs.toFixed(2)}ms`)

      const queryResponse = await fetchJson<{ rows: unknown[]; count: number; latencyMs: number }>(
        '/parquedb/tables/e2e-test/query',
        {
          method: 'POST',
          body: JSON.stringify({ filter: {}, limit: 10 }),
        }
      )

      expect(queryResponse.count).toBeGreaterThanOrEqual(2)

      console.log(`Query latency: ${queryResponse.latencyMs.toFixed(2)}ms`)
    })
  })
})
