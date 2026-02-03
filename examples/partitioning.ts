/**
 * Partitioning Example for @dotdo/deltalake
 *
 * This example demonstrates:
 * - Creating partitioned tables
 * - Writing data with partition columns
 * - Partition pruning during queries
 * - Multi-level partitioning
 * - Best practices for partition column selection
 *
 * Run with: npx tsx examples/partitioning.ts
 */

import {
  createStorage,
  DeltaTable,
  type Filter,
} from '../src/index.js'

// Define a log event with typical partition-friendly fields
// Using an index signature to satisfy Record<string, unknown> constraint
interface LogEvent {
  [key: string]: unknown
  eventId: string
  timestamp: Date
  level: 'debug' | 'info' | 'warn' | 'error'
  service: string
  message: string
  metadata?: Record<string, unknown>
  // Partition columns (derived from timestamp)
  year: number
  month: number
  day: number
}

// Helper to create log events with auto-populated partition columns
function createLogEvent(
  level: LogEvent['level'],
  service: string,
  message: string,
  date: Date = new Date()
): LogEvent {
  return {
    eventId: `log-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: date,
    level,
    service,
    message,
    year: date.getFullYear(),
    month: date.getMonth() + 1, // 1-indexed month
    day: date.getDate(),
  }
}

// Generate sample log data across multiple days
function generateSampleLogs(): LogEvent[] {
  const logs: LogEvent[] = []
  const services = ['api-gateway', 'auth-service', 'payment-service', 'user-service']
  const levels: LogEvent['level'][] = ['debug', 'info', 'warn', 'error']

  // Generate logs for the past 3 days
  for (let daysAgo = 0; daysAgo < 3; daysAgo++) {
    const date = new Date()
    date.setDate(date.getDate() - daysAgo)
    date.setHours(0, 0, 0, 0)

    // Generate 5-10 logs per day
    const logsPerDay = 5 + Math.floor(Math.random() * 6)

    for (let i = 0; i < logsPerDay; i++) {
      const service = services[Math.floor(Math.random() * services.length)]!
      const level = levels[Math.floor(Math.random() * levels.length)]!

      const logDate = new Date(date)
      logDate.setHours(Math.floor(Math.random() * 24))
      logDate.setMinutes(Math.floor(Math.random() * 60))

      logs.push(
        createLogEvent(
          level,
          service,
          `${level.toUpperCase()}: ${service} - Sample log message ${i + 1}`,
          logDate
        )
      )
    }
  }

  return logs.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime())
}

async function main() {
  console.log('=== Delta Lake Partitioning Example ===\n')

  // -----------------------------------------------------
  // 1. Create storage and partitioned table
  // -----------------------------------------------------
  const storage = createStorage({ type: 'memory' })
  const logsTable = new DeltaTable<LogEvent>(storage, 'logs')

  console.log('1. Created partitioned logs table')
  console.log('   Partition columns: [year, month, day]')

  // -----------------------------------------------------
  // 2. Write partitioned data
  // -----------------------------------------------------
  const sampleLogs = generateSampleLogs()
  console.log(`\n2. Writing ${sampleLogs.length} log events with partitioning...`)

  // Write with partition columns specified
  const commit = await logsTable.write(sampleLogs, {
    partitionColumns: ['year', 'month', 'day'],
  })

  console.log(`   Commit version: ${commit.version}`)
  console.log(`   Actions in commit: ${commit.actions.length}`)

  // Show partition distribution
  const partitionStats = new Map<string, number>()
  for (const log of sampleLogs) {
    const key = `${log.year}-${String(log.month).padStart(2, '0')}-${String(log.day).padStart(2, '0')}`
    partitionStats.set(key, (partitionStats.get(key) || 0) + 1)
  }

  console.log('\n   Partition distribution:')
  partitionStats.forEach((count, partition) => {
    console.log(`     ${partition}: ${count} events`)
  })

  // -----------------------------------------------------
  // 3. Query with partition pruning
  // -----------------------------------------------------
  console.log('\n3. Querying with partition pruning')

  // Get current date info for filtering
  const today = new Date()
  const todayYear = today.getFullYear()
  const todayMonth = today.getMonth() + 1
  const todayDay = today.getDate()

  // Query for today's logs - should use partition pruning
  const todayFilter: Filter<LogEvent> = {
    year: todayYear,
    month: todayMonth,
    day: todayDay,
  }

  const todayLogs = await logsTable.query(todayFilter)
  console.log(`\n   Query: year=${todayYear}, month=${todayMonth}, day=${todayDay}`)
  console.log(`   Results: ${todayLogs.length} events`)
  console.log(`   Files skipped by partition pruning: ${logsTable.lastQuerySkippedFiles ?? 0}`)

  // -----------------------------------------------------
  // 4. Query specific partition with additional filters
  // -----------------------------------------------------
  console.log('\n4. Partition filter combined with row-level filter')

  const errorFilter: Filter<LogEvent> = {
    year: todayYear,
    month: todayMonth,
    day: todayDay,
    level: 'error',
  }

  const todayErrors = await logsTable.query(errorFilter)
  console.log(`\n   Query: Today's errors`)
  console.log(`   Results: ${todayErrors.length} error events`)
  if (todayErrors.length > 0) {
    console.log('   Sample errors:')
    todayErrors.slice(0, 3).forEach(e => {
      console.log(`     - [${e.service}] ${e.message}`)
    })
  }

  // -----------------------------------------------------
  // 5. Query with $in for multiple partition values
  // -----------------------------------------------------
  console.log('\n5. Query multiple partition values with $in')

  // Query for multiple services
  const multiServiceFilter: Filter<LogEvent> = {
    service: { $in: ['api-gateway', 'auth-service'] },
  }

  const multiServiceLogs = await logsTable.query(multiServiceFilter)
  console.log(`\n   Query: service IN ['api-gateway', 'auth-service']`)
  console.log(`   Results: ${multiServiceLogs.length} events`)

  // Group by service to show distribution
  const serviceGroups = new Map<string, number>()
  multiServiceLogs.forEach(log => {
    const service = log.service!
    serviceGroups.set(service, (serviceGroups.get(service) || 0) + 1)
  })
  console.log('   Distribution:')
  serviceGroups.forEach((count, service) => {
    console.log(`     ${service}: ${count} events`)
  })

  // -----------------------------------------------------
  // 6. Query without partition columns (full scan)
  // -----------------------------------------------------
  console.log('\n6. Query without partition columns (requires full scan)')

  const allErrorsFilter: Filter<LogEvent> = {
    level: 'error',
  }

  const allErrors = await logsTable.query(allErrorsFilter)
  console.log(`\n   Query: level='error' (no partition filter)`)
  console.log(`   Results: ${allErrors.length} events across all partitions`)
  console.log(`   Note: This scans all files since no partition columns in filter`)

  // -----------------------------------------------------
  // 7. Examining table structure
  // -----------------------------------------------------
  console.log('\n7. Examining partitioned table structure')

  const snapshot = await logsTable.snapshot()
  console.log(`\n   Table version: ${snapshot.version}`)
  console.log(`   Number of data files: ${snapshot.files.length}`)
  console.log(`   Partition columns: ${snapshot.metadata?.partitionColumns?.join(', ') || 'none'}`)

  console.log('\n   Data files:')
  snapshot.files.forEach(file => {
    const partValues = file.partitionValues
      ? Object.entries(file.partitionValues)
          .map(([k, v]) => `${k}=${v}`)
          .join('/')
      : 'unpartitioned'
    console.log(`     ${partValues}`)
    console.log(`       path: ${file.path}`)
    console.log(`       size: ${file.size} bytes`)
  })

  // -----------------------------------------------------
  // 8. Appending to partitioned table
  // -----------------------------------------------------
  console.log('\n8. Appending data to existing partitions')

  // Add more logs for today
  const newLogs = [
    createLogEvent('error', 'payment-service', 'Payment processing timeout'),
    createLogEvent('warn', 'api-gateway', 'High latency detected'),
    createLogEvent('info', 'user-service', 'User login successful'),
  ]

  const commit2 = await logsTable.write(newLogs, {
    partitionColumns: ['year', 'month', 'day'],
  })

  console.log(`\n   Added ${newLogs.length} new log events`)
  console.log(`   New version: ${commit2.version}`)

  const updatedSnapshot = await logsTable.snapshot()
  console.log(`   Total data files now: ${updatedSnapshot.files.length}`)

  // -----------------------------------------------------
  // 9. Best Practices
  // -----------------------------------------------------
  console.log('\n9. Partitioning Best Practices')
  console.log(`
   Choose partition columns wisely:
   - Use columns frequently used in WHERE clauses
   - Avoid high-cardinality columns (e.g., user_id)
   - Date/time partitions (year/month/day) are common and effective
   - Consider query patterns: if you always query by region, partition by region

   Partition granularity:
   - Too few partitions = large files, slower queries
   - Too many partitions = small files (bad for Parquet), metadata overhead
   - Aim for 1GB-10GB per partition for optimal performance

   Common partition strategies:
   - Time-based: year/month/day for event logs
   - Geographic: region/country for location data
   - Categorical: status/type for business data
   - Composite: region/year/month for geographically distributed time-series

   This table uses: [year, month, day]
   - Good for time-range queries
   - Each day gets its own partition directory
   - Files: logs/year=2024/month=01/day=15/part-*.parquet
  `)

  // -----------------------------------------------------
  // Summary
  // -----------------------------------------------------
  console.log('=== Summary ===')
  console.log(`Total events: ${(await logsTable.query()).length}`)
  console.log(`Table version: ${await logsTable.version()}`)
  console.log(`Data files: ${(await logsTable.snapshot()).files.length}`)

  // Show final partition distribution
  const allLogs = await logsTable.query()
  const finalDistribution = new Map<string, number>()
  allLogs.forEach(log => {
    const key = `${log.year}-${String(log.month!).padStart(2, '0')}-${String(log.day!).padStart(2, '0')}`
    finalDistribution.set(key, (finalDistribution.get(key) || 0) + 1)
  })

  console.log('\nFinal partition distribution:')
  const sortedPartitions = Array.from(finalDistribution.entries()).sort((a, b) => a[0].localeCompare(b[0]))
  sortedPartitions.forEach(([partition, count]) => {
    console.log(`  ${partition}: ${count} events`)
  })

  console.log('\nPartitioning example completed successfully!')
}

// Run the example
main().catch(console.error)
