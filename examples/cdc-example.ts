/**
 * CDC (Change Data Capture) Example for @dotdo/deltalake
 *
 * This example demonstrates:
 * - Creating CDC producers to emit change records
 * - Creating CDC consumers to process changes
 * - Filtering CDC records by operation type
 * - Position tracking and checkpointing
 * - Snapshot operations for initial data sync
 *
 * Run with: npx tsx examples/cdc-example.ts
 */

import {
  CDCProducer,
  CDCConsumer,
  type CDCRecord,
  type CDCOperation,
} from '../src/index.js'

// Define the data type we're tracking
interface Order {
  orderId: string
  customerId: string
  items: Array<{ productId: string; quantity: number; price: number }>
  totalAmount: number
  status: 'pending' | 'confirmed' | 'shipped' | 'delivered' | 'cancelled'
  createdAt: string
  updatedAt: string
}

// Helper to format timestamps
function formatTimestamp(ts: bigint): string {
  // Convert from nanoseconds to milliseconds
  const ms = Number(ts / BigInt(1_000_000))
  return new Date(ms).toISOString()
}

// Helper to format operation type
function formatOp(op: CDCOperation): string {
  const ops: Record<CDCOperation, string> = {
    c: 'CREATE',
    u: 'UPDATE',
    d: 'DELETE',
    r: 'READ/SNAPSHOT',
  }
  return ops[op]
}

async function main() {
  console.log('=== Delta Lake CDC Example ===\n')

  // -----------------------------------------------------
  // 1. Create a CDC Producer
  // -----------------------------------------------------
  // The producer generates standardized CDC records for downstream consumers.
  // It's typically used when data changes occur in your system.

  const producer = new CDCProducer<Order>({
    source: {
      database: 'ecommerce',
      collection: 'orders',
    },
    system: 'mongolake', // Identifies the source system
  })

  console.log('1. Created CDC producer for orders collection')
  console.log(`   Initial sequence: ${producer.getSequence()}`)

  // -----------------------------------------------------
  // 2. Create a CDC Consumer
  // -----------------------------------------------------
  // The consumer processes CDC records and notifies subscribers.
  // You can filter by operation types and track position.

  const consumer = new CDCConsumer<Order>({
    // Only process creates and updates (skip deletes)
    operations: ['c', 'u'],
  })

  // Track processed records for this example
  const processedRecords: CDCRecord<Order>[] = []

  // Subscribe to receive CDC records
  const unsubscribe = consumer.subscribe(async record => {
    processedRecords.push(record)
    console.log(
      `   [Consumer] Received ${formatOp(record._op)}: ` +
        `${record._id} (seq: ${record._seq})`
    )
  })

  console.log('\n2. Created CDC consumer')
  console.log(`   Filtering operations: ['c', 'u'] (creates and updates only)`)
  console.log(`   Subscribers: ${consumer.getSubscriberCount()}`)

  // -----------------------------------------------------
  // 3. Emit CREATE records
  // -----------------------------------------------------
  console.log('\n3. Emitting CREATE records...')

  // Simulate creating new orders
  const order1: Order = {
    orderId: 'ORD-001',
    customerId: 'CUST-123',
    items: [
      { productId: 'PROD-A', quantity: 2, price: 29.99 },
      { productId: 'PROD-B', quantity: 1, price: 49.99 },
    ],
    totalAmount: 109.97,
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  }

  const order2: Order = {
    orderId: 'ORD-002',
    customerId: 'CUST-456',
    items: [{ productId: 'PROD-C', quantity: 3, price: 19.99 }],
    totalAmount: 59.97,
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  }

  // Emit CDC records for order creation
  const createRecord1 = await producer.create('ORD-001', order1)
  const createRecord2 = await producer.create('ORD-002', order2)

  // Process the records through the consumer
  await consumer.process(createRecord1)
  await consumer.process(createRecord2)

  console.log(`\n   Producer sequence after creates: ${producer.getSequence()}`)
  console.log(`   Consumer position: ${consumer.getPosition()}`)

  // -----------------------------------------------------
  // 4. Emit UPDATE records
  // -----------------------------------------------------
  console.log('\n4. Emitting UPDATE record...')

  // Order status changed from pending to confirmed
  const order1Updated: Order = {
    ...order1,
    status: 'confirmed',
    updatedAt: new Date().toISOString(),
  }

  const updateRecord = await producer.update('ORD-001', order1, order1Updated)
  await consumer.process(updateRecord)

  console.log(`   Before: status=${order1.status}`)
  console.log(`   After: status=${order1Updated.status}`)

  // -----------------------------------------------------
  // 5. Emit DELETE record (will be filtered by consumer)
  // -----------------------------------------------------
  console.log('\n5. Emitting DELETE record (will be filtered out)...')

  const deleteRecord = await producer.delete('ORD-002', order2)
  await consumer.process(deleteRecord) // This won't trigger the subscriber

  console.log(
    `   Delete record created (seq: ${deleteRecord._seq}), ` +
      `but consumer filters only ['c', 'u']`
  )
  console.log(`   Processed records count: ${processedRecords.length} (still 3, not 4)`)

  // -----------------------------------------------------
  // 6. Snapshot operation (bulk read)
  // -----------------------------------------------------
  console.log('\n6. Creating snapshot (bulk read) records...')

  // Snapshots are useful for initial data sync or point-in-time captures
  const existingOrders = [
    { id: 'ORD-100', data: { ...order1, orderId: 'ORD-100', status: 'delivered' as const } },
    { id: 'ORD-101', data: { ...order2, orderId: 'ORD-101', status: 'shipped' as const } },
  ]

  const snapshotRecords = await producer.snapshot(existingOrders)
  console.log(`   Created ${snapshotRecords.length} snapshot records`)
  snapshotRecords.forEach(r => {
    console.log(`   - ${r._id}: op=${formatOp(r._op)}, seq=${r._seq}`)
  })

  // -----------------------------------------------------
  // 7. Using emit() for custom operations
  // -----------------------------------------------------
  console.log('\n7. Using emit() for custom CDC records...')

  // emit() gives you full control over the CDC record
  const customRecord = await producer.emit(
    'u',
    'ORD-001',
    order1Updated,
    { ...order1Updated, status: 'shipped' },
    'TXN-789' // Optional transaction ID for exactly-once semantics
  )

  console.log(`   Custom record with transaction ID: ${customRecord._txn}`)
  console.log(`   Timestamp: ${formatTimestamp(customRecord._ts)}`)

  // -----------------------------------------------------
  // 8. Consumer position management
  // -----------------------------------------------------
  console.log('\n8. Consumer position management...')

  const currentPosition = consumer.getPosition()
  console.log(`   Current position: ${currentPosition}`)

  // Seek to a specific position (useful for recovery/replay)
  consumer.seekTo(BigInt(0))
  console.log(`   After seekTo(0): ${consumer.getPosition()}`)

  // You can also filter by timestamp
  const oneHourAgo = new Date(Date.now() - 3600000)
  consumer.seekToTimestamp(oneHourAgo)
  console.log(`   Set timestamp filter to: ${oneHourAgo.toISOString()}`)

  // -----------------------------------------------------
  // 9. Multiple subscribers
  // -----------------------------------------------------
  console.log('\n9. Multiple subscribers example...')

  let analyticsCount = 0
  let alertCount = 0

  // Analytics subscriber
  const unsubAnalytics = consumer.subscribe(async record => {
    analyticsCount++
    // In real code: send to analytics pipeline
  })

  // Alert subscriber (for high-value orders)
  const unsubAlert = consumer.subscribe(async record => {
    if (record._after && record._after.totalAmount > 100) {
      alertCount++
      // In real code: send alert notification
    }
  })

  console.log(`   Subscriber count: ${consumer.getSubscriberCount()}`)

  // Process a high-value order
  const bigOrder: Order = {
    orderId: 'ORD-003',
    customerId: 'CUST-789',
    items: [{ productId: 'PROD-PREMIUM', quantity: 1, price: 999.99 }],
    totalAmount: 999.99,
    status: 'pending',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  }

  // Reset position to process new records
  consumer.seekTo(BigInt(100))
  const bigOrderRecord = await producer.create('ORD-003', bigOrder)
  await consumer.process(bigOrderRecord)

  console.log(`   Analytics processed: ${analyticsCount}`)
  console.log(`   Alerts triggered: ${alertCount}`)

  // Clean up subscriptions
  unsubAnalytics()
  unsubAlert()
  console.log(`   After unsubscribing: ${consumer.getSubscriberCount()} subscribers`)

  // -----------------------------------------------------
  // 10. Examining CDC record structure
  // -----------------------------------------------------
  console.log('\n10. CDC Record structure example:')
  const sampleRecord = createRecord1
  console.log(
    JSON.stringify(
      {
        _id: sampleRecord._id,
        _seq: sampleRecord._seq.toString(), // Convert BigInt for JSON
        _op: sampleRecord._op,
        _before: sampleRecord._before,
        _after: {
          orderId: sampleRecord._after?.orderId,
          status: sampleRecord._after?.status,
          totalAmount: sampleRecord._after?.totalAmount,
        },
        _ts: formatTimestamp(sampleRecord._ts),
        _source: sampleRecord._source,
      },
      null,
      2
    )
  )

  // -----------------------------------------------------
  // Summary
  // -----------------------------------------------------
  console.log('\n=== Summary ===')
  console.log(`Producer sequence: ${producer.getSequence()}`)
  console.log(`Total CDC records created: ${Number(producer.getSequence())}`)
  console.log(`Consumer processed: ${processedRecords.length} records (filtered)`)

  // Unsubscribe the original subscriber
  unsubscribe()

  console.log('\nCDC example completed successfully!')
}

// Run the example
main().catch(console.error)
