/**
 * Storage Backends Example for @dotdo/deltalake
 *
 * This example demonstrates:
 * - Different storage backend options
 * - Creating storage from URLs
 * - Storage operations (read, write, list, delete)
 * - Conditional writes for concurrency control
 * - Byte-range reads for efficient Parquet access
 *
 * Run with: npx tsx examples/storage-backends.ts
 */

import {
  createStorage,
  parseStorageUrl,
  type StorageBackend,
  FileNotFoundError,
  VersionMismatchError,
} from '../src/index.js'

// Import storage classes directly for demonstration purposes
// In production code, you'd typically just use createStorage()
import {
  MemoryStorage,
  FileSystemStorage,
} from '../src/storage/index.js'

// Helper to demonstrate storage operations
async function demonstrateStorageOps(storage: StorageBackend, name: string) {
  console.log(`\n--- ${name} Operations ---`)

  const testPath = 'test/example.txt'
  const testData = new TextEncoder().encode('Hello, Delta Lake!')

  // Write data
  await storage.write(testPath, testData)
  console.log(`  write('${testPath}') - OK`)

  // Check existence
  const exists = await storage.exists(testPath)
  console.log(`  exists('${testPath}') = ${exists}`)

  // Read data
  const readData = await storage.read(testPath)
  const content = new TextDecoder().decode(readData)
  console.log(`  read('${testPath}') = "${content}"`)

  // Get file stats
  const stat = await storage.stat(testPath)
  console.log(`  stat('${testPath}') = { size: ${stat?.size}, lastModified: ${stat?.lastModified.toISOString()} }`)

  // List files
  const files = await storage.list('test/')
  console.log(`  list('test/') = [${files.map(f => `'${f}'`).join(', ')}]`)

  // Delete file
  await storage.delete(testPath)
  console.log(`  delete('${testPath}') - OK`)

  // Verify deletion
  const existsAfterDelete = await storage.exists(testPath)
  console.log(`  exists('${testPath}') after delete = ${existsAfterDelete}`)
}

async function main() {
  console.log('=== Delta Lake Storage Backends Example ===')

  // -----------------------------------------------------
  // 1. Memory Storage (for testing)
  // -----------------------------------------------------
  console.log('\n1. Memory Storage')
  console.log('   Best for: Unit tests, development, temporary data')

  const memoryStorage = createStorage({ type: 'memory' })
  await demonstrateStorageOps(memoryStorage, 'MemoryStorage')

  // Memory storage testing utilities
  const memStorage = memoryStorage as MemoryStorage
  console.log(`\n  Testing utilities:`)
  console.log(`    getFileCount() = ${memStorage.getFileCount()}`)
  console.log(`    getUsedSize() = ${memStorage.getUsedSize()} bytes`)

  // Snapshot and restore
  await memStorage.write('state/data.json', new TextEncoder().encode('{"version": 1}'))
  const snapshot = memStorage.snapshot()
  console.log(`    snapshot() - saved state with ${snapshot.files.size} file(s)`)

  await memStorage.write('state/data.json', new TextEncoder().encode('{"version": 2}'))
  memStorage.restore(snapshot)
  const restored = await memStorage.read('state/data.json')
  console.log(`    restore() - restored to: ${new TextDecoder().decode(restored)}`)

  // Clear all data
  memStorage.clear()
  console.log(`    clear() - cleared all data, fileCount = ${memStorage.getFileCount()}`)

  // -----------------------------------------------------
  // 2. FileSystem Storage (for local development)
  // -----------------------------------------------------
  console.log('\n2. FileSystem Storage')
  console.log('   Best for: Local development, on-premises deployments')

  // Note: This uses a temp directory to avoid creating files in the project
  const fsStorage = new FileSystemStorage({ path: '/tmp/deltalake-example' })

  console.log('   Path: /tmp/deltalake-example')
  await demonstrateStorageOps(fsStorage, 'FileSystemStorage')

  // Clean up temp directory
  await fsStorage.delete('test/example.txt')

  // -----------------------------------------------------
  // 3. Creating Storage from URLs
  // -----------------------------------------------------
  console.log('\n3. Creating Storage from URLs')
  console.log('   Supported URL schemes:')

  // Parse different URL formats
  const urlExamples = [
    'memory://',
    'file:///data/lake',
    '/data/lake',
    './local/data',
    's3://my-bucket/prefix',
    's3://my-bucket.s3.us-west-2.amazonaws.com/prefix',
    'r2://my-bucket/prefix',
  ]

  for (const url of urlExamples) {
    try {
      const parsed = parseStorageUrl(url)
      console.log(`   ${url}`)
      console.log(`     -> type: ${parsed.type}, path: ${parsed.path}${parsed.bucket ? `, bucket: ${parsed.bucket}` : ''}${parsed.region ? `, region: ${parsed.region}` : ''}`)
    } catch (e) {
      console.log(`   ${url} -> ERROR: ${(e as Error).message}`)
    }
  }

  // Create storage directly from URL
  console.log('\n   Creating from URL:')
  const storageFromUrl = createStorage('memory://')
  console.log(`   createStorage('memory://') - OK`)

  // -----------------------------------------------------
  // 4. Conditional Writes (Optimistic Concurrency)
  // -----------------------------------------------------
  console.log('\n4. Conditional Writes (Optimistic Concurrency)')
  console.log('   Essential for ACID transactions in Delta Lake')

  const concurrencyStorage = createStorage({ type: 'memory' })
  const filePath = 'delta_log/00000000000000000000.json'

  // Create file with conditional write (expectedVersion = null means create-if-not-exists)
  const version1 = await concurrencyStorage.writeConditional(
    filePath,
    new TextEncoder().encode('{"version": 0}'),
    null // Expected version: null = file should not exist
  )
  console.log(`\n   writeConditional() with expectedVersion=null`)
  console.log(`   Created file with version: ${version1}`)

  // Update with correct version
  const version2 = await concurrencyStorage.writeConditional(
    filePath,
    new TextEncoder().encode('{"version": 1}'),
    version1 // Expected version must match
  )
  console.log(`\n   writeConditional() with expectedVersion='${version1}'`)
  console.log(`   Updated file, new version: ${version2}`)

  // Attempt update with stale version (simulates concurrent write)
  console.log(`\n   writeConditional() with stale expectedVersion='${version1}'`)
  try {
    await concurrencyStorage.writeConditional(
      filePath,
      new TextEncoder().encode('{"version": 2}'),
      version1 // This is now stale
    )
  } catch (e) {
    if (e instanceof VersionMismatchError) {
      console.log(`   VersionMismatchError caught!`)
      console.log(`     Expected: ${e.expectedVersion}`)
      console.log(`     Actual: ${e.actualVersion}`)
    }
  }

  // Attempt to create file that already exists
  console.log(`\n   writeConditional() create on existing file`)
  try {
    await concurrencyStorage.writeConditional(
      filePath,
      new TextEncoder().encode('{"version": 0}'),
      null // Expected: file doesn't exist
    )
  } catch (e) {
    if (e instanceof VersionMismatchError) {
      console.log(`   VersionMismatchError: File already exists (version: ${e.actualVersion})`)
    }
  }

  // -----------------------------------------------------
  // 5. Byte-Range Reads (for Parquet files)
  // -----------------------------------------------------
  console.log('\n5. Byte-Range Reads')
  console.log('   Efficient for reading Parquet metadata without downloading entire file')

  const rangeStorage = createStorage({ type: 'memory' })

  // Simulate a large file (in reality this would be a Parquet file)
  const largeData = new TextEncoder().encode(
    'HEADER_DATA_' + // First 12 bytes
      'x'.repeat(1000) + // Middle content
      '_FOOTER_META' // Last 12 bytes (Parquet metadata is at the end)
  )
  await rangeStorage.write('data/large-file.parquet', largeData)

  console.log(`\n   File size: ${largeData.length} bytes`)

  // Read just the header
  const header = await rangeStorage.readRange('data/large-file.parquet', 0, 12)
  console.log(`   readRange(0, 12) = "${new TextDecoder().decode(header)}"`)

  // Read just the footer (Parquet stores metadata at the end)
  const footer = await rangeStorage.readRange('data/large-file.parquet', largeData.length - 12, largeData.length)
  console.log(`   readRange(${largeData.length - 12}, ${largeData.length}) = "${new TextDecoder().decode(footer)}"`)

  // -----------------------------------------------------
  // 6. Error Handling
  // -----------------------------------------------------
  console.log('\n6. Error Handling')

  const errorStorage = createStorage({ type: 'memory' })

  // FileNotFoundError on read
  try {
    await errorStorage.read('nonexistent/file.txt')
  } catch (e) {
    if (e instanceof FileNotFoundError) {
      console.log(`   FileNotFoundError: ${e.message}`)
      console.log(`     path: ${e.path}`)
      console.log(`     operation: ${e.operation}`)
    }
  }

  // exists() returns false instead of throwing
  const notExists = await errorStorage.exists('nonexistent/file.txt')
  console.log(`   exists('nonexistent/file.txt') = ${notExists} (no error)`)

  // stat() returns null instead of throwing
  const noStat = await errorStorage.stat('nonexistent/file.txt')
  console.log(`   stat('nonexistent/file.txt') = ${noStat} (no error)`)

  // delete() is idempotent (no error on nonexistent file)
  await errorStorage.delete('nonexistent/file.txt')
  console.log(`   delete('nonexistent/file.txt') - OK (idempotent)`)

  // -----------------------------------------------------
  // 7. Storage Backend Configuration Examples
  // -----------------------------------------------------
  console.log('\n7. Storage Configuration Examples')

  console.log('\n   Memory Storage with options:')
  console.log(`   new MemoryStorage({ maxSize: 1024 * 1024 }) // 1MB limit`)
  console.log(`   new MemoryStorage({ latency: { read: 10, write: 50 } }) // Simulate latency`)

  console.log('\n   FileSystem Storage:')
  console.log(`   createStorage({ type: 'filesystem', path: './data' })`)
  console.log(`   createStorage('file:///absolute/path/to/data')`)
  console.log(`   createStorage('./relative/path')`)

  console.log('\n   S3 Storage (AWS):')
  console.log(`   createStorage({`)
  console.log(`     type: 's3',`)
  console.log(`     bucket: 'my-bucket',`)
  console.log(`     region: 'us-east-1',`)
  console.log(`     credentials: { accessKeyId: '...', secretAccessKey: '...' }`)
  console.log(`   })`)

  console.log('\n   R2 Storage (Cloudflare Workers):')
  console.log(`   createStorage({ type: 'r2', bucket: env.MY_BUCKET })`)

  // -----------------------------------------------------
  // Summary
  // -----------------------------------------------------
  console.log('\n=== Summary ===')
  console.log('Storage backends provide a unified interface for Delta Lake operations:')
  console.log('  - MemoryStorage: Fast, ephemeral storage for testing')
  console.log('  - FileSystemStorage: Local disk storage for development')
  console.log('  - S3Storage: AWS S3 for production cloud deployments')
  console.log('  - R2Storage: Cloudflare R2 for edge deployments')
  console.log('')
  console.log('Key features:')
  console.log('  - writeConditional(): Enables optimistic concurrency control')
  console.log('  - readRange(): Efficient partial file reads for Parquet')
  console.log('  - Consistent error types across all backends')

  console.log('\nStorage backends example completed successfully!')
}

// Run the example
main().catch(console.error)
