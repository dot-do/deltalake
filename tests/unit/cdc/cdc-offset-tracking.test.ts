/**
 * CDC Consumer Offset Tracking Tests
 *
 * Tests for explicit consumer offset tracking functionality
 * that enables Kafka-like consumer semantics.
 *
 * Features tested:
 * - Manual offset commit (commitOffset, commitCurrentOffset)
 * - Offset retrieval (getCommittedOffset)
 * - Resume from committed offset
 * - Offset reset
 * - Auto-commit functionality
 * - Consumer group and partition support
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import {
  CDCConsumer,
  CDCRecord,
  CDCProducer,
  MemoryOffsetStorage,
  ConsumerOffset,
  OffsetStorage,
} from '../../../src/cdc/index'
import { CDCError } from '../../../src/errors'

// =============================================================================
// TEST UTILITIES
// =============================================================================

interface TestData {
  id: string
  name: string
  value: number
}

/**
 * Create a mock CDC record for testing
 */
function createTestRecord(seq: bigint, data: TestData): CDCRecord<TestData> {
  return {
    _id: data.id,
    _seq: seq,
    _op: 'c',
    _before: null,
    _after: data,
    _ts: BigInt(Date.now()) * 1_000_000n,
    _source: {
      system: 'kafkalake',
      database: 'test',
      collection: 'events',
    },
  }
}

// =============================================================================
// MemoryOffsetStorage TESTS
// =============================================================================

describe('MemoryOffsetStorage', () => {
  let storage: MemoryOffsetStorage

  beforeEach(() => {
    storage = new MemoryOffsetStorage()
  })

  describe('getOffset / commitOffset', () => {
    it('should return null when no offset is committed', async () => {
      const offset = await storage.getOffset('group1', 'topic1')
      expect(offset).toBeNull()
    })

    it('should commit and retrieve offset', async () => {
      const consumerOffset: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
        metadata: 'test-metadata',
      }

      await storage.commitOffset('group1', 'topic1', consumerOffset)
      const retrieved = await storage.getOffset('group1', 'topic1')

      expect(retrieved).not.toBeNull()
      expect(retrieved!.offset).toBe(100n)
      expect(retrieved!.partition).toBe(0)
      expect(retrieved!.metadata).toBe('test-metadata')
    })

    it('should handle different partitions separately', async () => {
      const offset0: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }
      const offset1: ConsumerOffset = {
        offset: 200n,
        partition: 1,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset0, 0)
      await storage.commitOffset('group1', 'topic1', offset1, 1)

      const retrieved0 = await storage.getOffset('group1', 'topic1', 0)
      const retrieved1 = await storage.getOffset('group1', 'topic1', 1)

      expect(retrieved0!.offset).toBe(100n)
      expect(retrieved1!.offset).toBe(200n)
    })

    it('should handle different consumer groups separately', async () => {
      const offset1: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }
      const offset2: ConsumerOffset = {
        offset: 200n,
        partition: 0,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset1)
      await storage.commitOffset('group2', 'topic1', offset2)

      const retrievedGroup1 = await storage.getOffset('group1', 'topic1')
      const retrievedGroup2 = await storage.getOffset('group2', 'topic1')

      expect(retrievedGroup1!.offset).toBe(100n)
      expect(retrievedGroup2!.offset).toBe(200n)
    })

    it('should update existing offset on re-commit', async () => {
      const offset1: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }
      const offset2: ConsumerOffset = {
        offset: 200n,
        partition: 0,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset1)
      await storage.commitOffset('group1', 'topic1', offset2)

      const retrieved = await storage.getOffset('group1', 'topic1')
      expect(retrieved!.offset).toBe(200n)
    })
  })

  describe('deleteOffset', () => {
    it('should delete committed offset', async () => {
      const offset: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset)
      expect(await storage.getOffset('group1', 'topic1')).not.toBeNull()

      await storage.deleteOffset('group1', 'topic1')
      expect(await storage.getOffset('group1', 'topic1')).toBeNull()
    })

    it('should not throw when deleting non-existent offset', async () => {
      await expect(storage.deleteOffset('group1', 'topic1')).resolves.not.toThrow()
    })
  })

  describe('listOffsets', () => {
    it('should list all offsets for a consumer group', async () => {
      const offset1: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }
      const offset2: ConsumerOffset = {
        offset: 200n,
        partition: 0,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset1)
      await storage.commitOffset('group1', 'topic2', offset2)
      await storage.commitOffset('group2', 'topic1', offset1) // Different group

      const offsets = await storage.listOffsets('group1')

      expect(offsets.size).toBe(2)
      expect([...offsets.keys()]).toContain('group1:topic1:0')
      expect([...offsets.keys()]).toContain('group1:topic2:0')
    })

    it('should return empty map for group with no offsets', async () => {
      const offsets = await storage.listOffsets('nonexistent-group')
      expect(offsets.size).toBe(0)
    })
  })

  describe('clear', () => {
    it('should clear all stored offsets', async () => {
      const offset: ConsumerOffset = {
        offset: 100n,
        partition: 0,
        committedAt: new Date(),
      }

      await storage.commitOffset('group1', 'topic1', offset)
      await storage.commitOffset('group2', 'topic2', offset)

      storage.clear()

      expect(await storage.getOffset('group1', 'topic1')).toBeNull()
      expect(await storage.getOffset('group2', 'topic2')).toBeNull()
    })
  })
})

// =============================================================================
// CDCConsumer OFFSET TRACKING TESTS
// =============================================================================

describe('CDCConsumer Offset Tracking', () => {
  let storage: MemoryOffsetStorage

  beforeEach(() => {
    storage = new MemoryOffsetStorage()
  })

  describe('Offset tracking configuration', () => {
    it('should report offset tracking as disabled without configuration', () => {
      const consumer = new CDCConsumer<TestData>()
      expect(consumer.isOffsetTrackingEnabled()).toBe(false)
    })

    it('should report offset tracking as enabled with full configuration', () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })
      expect(consumer.isOffsetTrackingEnabled()).toBe(true)
    })

    it('should report offset tracking as disabled with partial configuration', () => {
      const consumer1 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        // Missing topic and offsetStorage
      })
      expect(consumer1.isOffsetTrackingEnabled()).toBe(false)

      const consumer2 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        // Missing offsetStorage
      })
      expect(consumer2.isOffsetTrackingEnabled()).toBe(false)
    })

    it('should expose configuration getters', () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'my-group',
        topic: 'my-topic',
        partition: 5,
        offsetStorage: storage,
      })

      expect(consumer.getGroupId()).toBe('my-group')
      expect(consumer.getTopic()).toBe('my-topic')
      expect(consumer.getPartition()).toBe(5)
    })

    it('should use default partition 0', () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'my-group',
        topic: 'my-topic',
        offsetStorage: storage,
      })

      expect(consumer.getPartition()).toBe(0)
    })
  })

  describe('commitOffset', () => {
    it('should throw when groupId is not configured', async () => {
      const consumer = new CDCConsumer<TestData>({
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await expect(consumer.commitOffset(100n)).rejects.toThrow(CDCError)
      await expect(consumer.commitOffset(100n)).rejects.toThrow('groupId')
    })

    it('should throw when topic is not configured', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        offsetStorage: storage,
      })

      await expect(consumer.commitOffset(100n)).rejects.toThrow(CDCError)
      await expect(consumer.commitOffset(100n)).rejects.toThrow('topic')
    })

    it('should throw when offsetStorage is not configured', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
      })

      await expect(consumer.commitOffset(100n)).rejects.toThrow(CDCError)
      await expect(consumer.commitOffset(100n)).rejects.toThrow('storage')
    })

    it('should commit offset successfully', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const committed = await consumer.commitOffset(100n)

      expect(committed.offset).toBe(100n)
      expect(committed.committedAt).toBeInstanceOf(Date)

      // Verify it was stored
      const stored = await storage.getOffset('test-group', 'test-topic')
      expect(stored!.offset).toBe(100n)
    })

    it('should commit with metadata', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const committed = await consumer.commitOffset(100n, {
        metadata: 'custom-metadata',
      })

      expect(committed.metadata).toBe('custom-metadata')
    })

    it('should support asynchronous commit', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const committed = await consumer.commitOffset(100n, { sync: false })

      expect(committed.offset).toBe(100n)

      // Wait a bit for async operation
      await new Promise(resolve => setTimeout(resolve, 10))

      const stored = await storage.getOffset('test-group', 'test-topic')
      expect(stored!.offset).toBe(100n)
    })
  })

  describe('commitCurrentOffset', () => {
    it('should commit current position', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
        fromSeq: 50n,
      })

      // Process a record to advance position
      const record = createTestRecord(50n, { id: '1', name: 'Test', value: 100 })
      consumer.subscribe(async () => {})
      await consumer.process(record)

      // Position should now be 51n
      expect(consumer.getPosition()).toBe(51n)

      const committed = await consumer.commitCurrentOffset()
      expect(committed.offset).toBe(51n)
    })
  })

  describe('getCommittedOffset', () => {
    it('should return null when no offset is committed', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const offset = await consumer.getCommittedOffset()
      expect(offset).toBeNull()
    })

    it('should return committed offset', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer.commitOffset(100n)
      const offset = await consumer.getCommittedOffset()

      expect(offset).not.toBeNull()
      expect(offset!.offset).toBe(100n)
    })

    it('should use cached offset by default', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer.commitOffset(100n)

      // Directly modify storage to simulate external change
      await storage.commitOffset('test-group', 'test-topic', {
        offset: 200n,
        partition: 0,
        committedAt: new Date(),
      })

      // Should return cached value (100n), not storage value (200n)
      const offset = await consumer.getCommittedOffset()
      expect(offset!.offset).toBe(100n)
    })

    it('should refresh from storage when forceRefresh is true', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer.commitOffset(100n)

      // Directly modify storage
      await storage.commitOffset('test-group', 'test-topic', {
        offset: 200n,
        partition: 0,
        committedAt: new Date(),
      })

      // Should return storage value (200n) when forceRefresh is true
      const offset = await consumer.getCommittedOffset(true)
      expect(offset!.offset).toBe(200n)
    })
  })

  describe('resumeFromCommittedOffset', () => {
    it('should return null and not change position when no offset committed', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
        fromSeq: 50n,
      })

      const resumed = await consumer.resumeFromCommittedOffset()

      expect(resumed).toBeNull()
      expect(consumer.getPosition()).toBe(50n) // Unchanged
    })

    it('should seek to committed offset position', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      // Pre-commit an offset
      await storage.commitOffset('test-group', 'test-topic', {
        offset: 150n,
        partition: 0,
        committedAt: new Date(),
      })

      const resumed = await consumer.resumeFromCommittedOffset()

      expect(resumed).not.toBeNull()
      expect(resumed!.offset).toBe(150n)
      expect(consumer.getPosition()).toBe(150n)
    })
  })

  describe('resetOffset', () => {
    it('should delete committed offset from storage', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer.commitOffset(100n)
      expect(await storage.getOffset('test-group', 'test-topic')).not.toBeNull()

      await consumer.resetOffset()
      expect(await storage.getOffset('test-group', 'test-topic')).toBeNull()
    })

    it('should clear cached offset', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer.commitOffset(100n)
      await consumer.resetOffset()

      // Getting committed offset should now return null (from cache)
      const offset = await consumer.getCommittedOffset()
      expect(offset).toBeNull()
    })
  })

  describe('setOffsetStorage', () => {
    it('should allow changing offset storage after construction', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
      })

      // Initially not enabled
      expect(consumer.isOffsetTrackingEnabled()).toBe(false)

      consumer.setOffsetStorage(storage)

      // Now enabled
      expect(consumer.isOffsetTrackingEnabled()).toBe(true)

      // Should be able to commit
      await expect(consumer.commitOffset(100n)).resolves.toBeDefined()
    })
  })

  describe('Auto-commit', () => {
    it('should auto-commit after processing when enabled', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
        enableAutoCommit: true,
      })

      consumer.subscribe(async () => {})

      const record = createTestRecord(0n, { id: '1', name: 'Test', value: 100 })
      await consumer.process(record)

      // Wait for async commit
      await new Promise(resolve => setTimeout(resolve, 20))

      const committed = await storage.getOffset('test-group', 'test-topic')
      expect(committed).not.toBeNull()
      expect(committed!.offset).toBe(1n)
    })

    it('should not auto-commit when disabled (default)', async () => {
      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
        // enableAutoCommit not set (defaults to false)
      })

      consumer.subscribe(async () => {})

      const record = createTestRecord(0n, { id: '1', name: 'Test', value: 100 })
      await consumer.process(record)

      await new Promise(resolve => setTimeout(resolve, 20))

      const committed = await storage.getOffset('test-group', 'test-topic')
      expect(committed).toBeNull()
    })

    it('should respect auto-commit interval', async () => {
      vi.useFakeTimers()

      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
        enableAutoCommit: true,
        autoCommitIntervalMs: 1000, // 1 second interval
      })

      consumer.subscribe(async () => {})

      // Process first record at t=0
      const record1 = createTestRecord(0n, { id: '1', name: 'Test1', value: 100 })
      await consumer.process(record1)

      // First commit should happen (no previous commit time)
      await vi.runAllTimersAsync()
      let committed = await storage.getOffset('test-group', 'test-topic')
      expect(committed).not.toBeNull()

      // Advance time by 500ms and process another record
      vi.advanceTimersByTime(500)
      const record2 = createTestRecord(1n, { id: '2', name: 'Test2', value: 200 })
      await consumer.process(record2)

      // Should NOT have committed yet (only 500ms elapsed)
      await vi.runAllTimersAsync()
      committed = await storage.getOffset('test-group', 'test-topic')
      expect(committed!.offset).toBe(1n) // Still at first commit

      // Advance another 600ms (total 1100ms from first commit)
      vi.advanceTimersByTime(600)
      const record3 = createTestRecord(2n, { id: '3', name: 'Test3', value: 300 })
      await consumer.process(record3)

      // Now should have committed (>1000ms elapsed)
      await vi.runAllTimersAsync()
      committed = await storage.getOffset('test-group', 'test-topic')
      expect(committed!.offset).toBe(3n)

      vi.useRealTimers()
    })

    it('should not throw when auto-commit fails', async () => {
      // Create a storage that fails on commit
      const failingStorage: OffsetStorage = {
        async getOffset() {
          return null
        },
        async commitOffset() {
          throw new Error('Storage failure')
        },
        async deleteOffset() {},
        async listOffsets() {
          return new Map()
        },
      }

      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: failingStorage,
        enableAutoCommit: true,
      })

      consumer.subscribe(async () => {})

      const record = createTestRecord(0n, { id: '1', name: 'Test', value: 100 })

      // Should not throw even though commit fails
      await expect(consumer.process(record)).resolves.not.toThrow()
    })
  })

  describe('Integration: process and resume', () => {
    it('should correctly resume processing after restart', async () => {
      // Simulate first consumer instance
      const consumer1 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const received1: string[] = []
      consumer1.subscribe(async (record) => {
        received1.push(record._id)
      })

      // Process records 0-4
      for (let i = 0; i < 5; i++) {
        const record = createTestRecord(BigInt(i), {
          id: `${i}`,
          name: `Test${i}`,
          value: i * 100,
        })
        await consumer1.process(record)
      }

      // Commit after processing
      await consumer1.commitCurrentOffset()

      expect(received1).toEqual(['0', '1', '2', '3', '4'])
      expect(consumer1.getPosition()).toBe(5n)

      // Simulate restart with new consumer instance
      const consumer2 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      // Resume from committed offset
      const resumed = await consumer2.resumeFromCommittedOffset()
      expect(resumed!.offset).toBe(5n)

      const received2: string[] = []
      consumer2.subscribe(async (record) => {
        received2.push(record._id)
      })

      // Process records 3-7 (some duplicates, some new)
      for (let i = 3; i < 8; i++) {
        const record = createTestRecord(BigInt(i), {
          id: `${i}`,
          name: `Test${i}`,
          value: i * 100,
        })
        await consumer2.process(record)
      }

      // Should only receive records >= committed offset (5n)
      expect(received2).toEqual(['5', '6', '7'])
    })

    it('should handle different partitions independently', async () => {
      const consumer0 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        partition: 0,
        offsetStorage: storage,
      })

      const consumer1 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        partition: 1,
        offsetStorage: storage,
      })

      consumer0.subscribe(async () => {})
      consumer1.subscribe(async () => {})

      // Process different amounts on each partition
      for (let i = 0; i < 10; i++) {
        await consumer0.process(createTestRecord(BigInt(i), { id: `${i}`, name: 'p0', value: i }))
      }
      for (let i = 0; i < 5; i++) {
        await consumer1.process(createTestRecord(BigInt(i), { id: `${i}`, name: 'p1', value: i }))
      }

      await consumer0.commitCurrentOffset()
      await consumer1.commitCurrentOffset()

      // Verify offsets are independent
      const offset0 = await storage.getOffset('test-group', 'test-topic', 0)
      const offset1 = await storage.getOffset('test-group', 'test-topic', 1)

      expect(offset0!.offset).toBe(10n)
      expect(offset1!.offset).toBe(5n)
    })

    it('should work with CDCProducer for end-to-end flow', async () => {
      const producer = new CDCProducer<TestData>({
        source: { database: 'test', collection: 'events' },
        system: 'kafkalake',
      })

      const consumer = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      const received: TestData[] = []
      consumer.subscribe(async (record) => {
        if (record._after) {
          received.push(record._after)
        }
      })

      // Produce and consume records
      const record1 = await producer.create('1', { id: '1', name: 'Alice', value: 100 })
      const record2 = await producer.create('2', { id: '2', name: 'Bob', value: 200 })
      const record3 = await producer.create('3', { id: '3', name: 'Charlie', value: 300 })

      await consumer.process(record1)
      await consumer.process(record2)
      await consumer.commitCurrentOffset()

      // "Restart" by creating new consumer
      const consumer2 = new CDCConsumer<TestData>({
        groupId: 'test-group',
        topic: 'test-topic',
        offsetStorage: storage,
      })

      await consumer2.resumeFromCommittedOffset()

      const received2: TestData[] = []
      consumer2.subscribe(async (record) => {
        if (record._after) {
          received2.push(record._after)
        }
      })

      // Replay all records to new consumer
      await consumer2.process(record1) // Should be skipped (seq 0)
      await consumer2.process(record2) // Should be skipped (seq 1)
      await consumer2.process(record3) // Should be processed (seq 2)

      expect(received).toHaveLength(2)
      expect(received2).toHaveLength(1)
      expect(received2[0].name).toBe('Charlie')
    })
  })
})
