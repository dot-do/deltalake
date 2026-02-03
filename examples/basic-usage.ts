/**
 * Basic Usage Example for @dotdo/deltalake
 *
 * This example demonstrates the core Delta Lake operations:
 * - Creating a storage backend
 * - Creating and writing to a Delta table
 * - Querying data with filters
 * - Using time travel to query historical versions
 *
 * Run with: npx tsx examples/basic-usage.ts
 */

import {
  createStorage,
  DeltaTable,
  type Filter,
} from '../src/index.js'

// Define the shape of our data
// Using an index signature to satisfy Record<string, unknown> constraint
interface UserEvent {
  [key: string]: unknown
  _id: string
  userId: string
  eventType: 'page_view' | 'click' | 'purchase' | 'signup'
  timestamp: number
  amount?: number
  metadata?: {
    page?: string
    productId?: string
    referrer?: string
  }
}

async function main() {
  console.log('=== Delta Lake Basic Usage Example ===\n')

  // -----------------------------------------------------
  // 1. Create a storage backend
  // -----------------------------------------------------
  // Using in-memory storage for this example.
  // In production, you would use:
  //   - createStorage({ type: 'filesystem', path: './data' }) for local files
  //   - createStorage({ type: 'r2', bucket: env.MY_BUCKET }) for Cloudflare R2
  //   - createStorage({ type: 's3', bucket: 'my-bucket', region: 'us-east-1' }) for AWS S3

  const storage = createStorage({ type: 'memory' })
  console.log('1. Created in-memory storage backend')

  // -----------------------------------------------------
  // 2. Create a Delta table
  // -----------------------------------------------------
  const table = new DeltaTable<UserEvent>(storage, 'events')
  console.log('2. Created Delta table: events')

  // Check initial version (should be -1 for new table)
  const initialVersion = await table.version()
  console.log(`   Initial table version: ${initialVersion}`)

  // -----------------------------------------------------
  // 3. Write initial data
  // -----------------------------------------------------
  const initialEvents: UserEvent[] = [
    {
      _id: 'evt-001',
      userId: 'user-alice',
      eventType: 'signup',
      timestamp: Date.now() - 86400000, // 1 day ago
      metadata: { referrer: 'google' },
    },
    {
      _id: 'evt-002',
      userId: 'user-alice',
      eventType: 'page_view',
      timestamp: Date.now() - 82800000,
      metadata: { page: '/products' },
    },
    {
      _id: 'evt-003',
      userId: 'user-bob',
      eventType: 'signup',
      timestamp: Date.now() - 43200000, // 12 hours ago
      metadata: { referrer: 'twitter' },
    },
    {
      _id: 'evt-004',
      userId: 'user-alice',
      eventType: 'purchase',
      timestamp: Date.now() - 3600000, // 1 hour ago
      amount: 99.99,
      metadata: { productId: 'prod-123' },
    },
    {
      _id: 'evt-005',
      userId: 'user-bob',
      eventType: 'click',
      timestamp: Date.now() - 1800000, // 30 mins ago
      metadata: { page: '/checkout' },
    },
  ]

  const commit1 = await table.write(initialEvents)
  console.log(`\n3. Wrote ${initialEvents.length} events`)
  console.log(`   New version: ${commit1.version}`)
  console.log(`   Commit timestamp: ${new Date(commit1.timestamp).toISOString()}`)

  // -----------------------------------------------------
  // 4. Query all data
  // -----------------------------------------------------
  const allEvents = await table.query()
  console.log(`\n4. Query all events: ${allEvents.length} rows`)

  // -----------------------------------------------------
  // 5. Query with equality filter
  // -----------------------------------------------------
  const aliceEvents = await table.query({ userId: 'user-alice' })
  console.log(`\n5. Events for user-alice: ${aliceEvents.length} rows`)
  aliceEvents.forEach(e => console.log(`   - ${e.eventType} at ${new Date(e.timestamp!).toISOString()}`))

  // -----------------------------------------------------
  // 6. Query with comparison operators
  // -----------------------------------------------------
  // Find events from the last 2 hours
  const recentThreshold = Date.now() - 7200000
  const recentFilter: Filter<UserEvent> = {
    timestamp: { $gte: recentThreshold },
  }
  const recentEvents = await table.query(recentFilter)
  console.log(`\n6. Recent events (last 2 hours): ${recentEvents.length} rows`)
  recentEvents.forEach(e => console.log(`   - ${e.userId}: ${e.eventType}`))

  // -----------------------------------------------------
  // 7. Query with $in operator
  // -----------------------------------------------------
  const interactionFilter: Filter<UserEvent> = {
    eventType: { $in: ['click', 'purchase'] },
  }
  const interactions = await table.query(interactionFilter)
  console.log(`\n7. Interaction events (click/purchase): ${interactions.length} rows`)

  // -----------------------------------------------------
  // 8. Query with logical operators
  // -----------------------------------------------------
  const complexFilter: Filter<UserEvent> = {
    $or: [
      { eventType: 'purchase' },
      { $and: [{ eventType: 'signup' }, { userId: 'user-bob' }] },
    ],
  }
  const complexResults = await table.query(complexFilter)
  console.log(`\n8. Complex query (purchases OR Bob's signup): ${complexResults.length} rows`)
  complexResults.forEach(e => console.log(`   - ${e.userId}: ${e.eventType}`))

  // -----------------------------------------------------
  // 9. Write more data (creates version 1)
  // -----------------------------------------------------
  const moreEvents: UserEvent[] = [
    {
      _id: 'evt-006',
      userId: 'user-charlie',
      eventType: 'signup',
      timestamp: Date.now() - 600000, // 10 mins ago
      metadata: { referrer: 'direct' },
    },
    {
      _id: 'evt-007',
      userId: 'user-charlie',
      eventType: 'purchase',
      timestamp: Date.now() - 300000, // 5 mins ago
      amount: 149.99,
      metadata: { productId: 'prod-456' },
    },
  ]

  const commit2 = await table.write(moreEvents)
  console.log(`\n9. Wrote ${moreEvents.length} more events`)
  console.log(`   New version: ${commit2.version}`)

  // -----------------------------------------------------
  // 10. Time travel - query historical version
  // -----------------------------------------------------
  // Query version 0 (before the second write)
  const historicalEvents = await table.query({}, { version: 0 })
  const currentEvents = await table.query()

  console.log(`\n10. Time travel comparison:`)
  console.log(`    Version 0 (historical): ${historicalEvents.length} events`)
  console.log(`    Version 1 (current): ${currentEvents.length} events`)

  // -----------------------------------------------------
  // 11. Using snapshots for consistent reads
  // -----------------------------------------------------
  const snapshot = await table.snapshot()
  console.log(`\n11. Created snapshot at version ${snapshot.version}`)

  // Multiple queries against the same snapshot
  const signups = await table.query({ eventType: 'signup' }, { snapshot })
  const purchases = await table.query({ eventType: 'purchase' }, { snapshot })
  console.log(`    Signups: ${signups.length}, Purchases: ${purchases.length}`)

  // -----------------------------------------------------
  // 12. Query with projection (select specific fields)
  // -----------------------------------------------------
  const projected = await table.query(
    { eventType: 'purchase' },
    { projection: ['_id', 'userId', 'amount'] }
  )
  console.log(`\n12. Projected query (purchases with _id, userId, amount):`)
  projected.forEach(e => console.log(`    - ${e._id}: $${e.amount} by ${e.userId}`))

  // -----------------------------------------------------
  // Summary
  // -----------------------------------------------------
  console.log('\n=== Summary ===')
  console.log(`Total events in table: ${currentEvents.length}`)
  console.log(`Table version: ${await table.version()}`)
  console.log(`Table has ${snapshot.files.length} data file(s)`)

  console.log('\nExample completed successfully!')
}

// Run the example
main().catch(console.error)
