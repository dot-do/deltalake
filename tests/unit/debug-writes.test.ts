import { describe, it, expect } from 'vitest'
import { MemoryStorage } from '../../src/storage/index.js'
import { DeltaTable } from '../../src/delta/index.js'

describe('debug multiple writes', () => {
  it('should handle multiple sequential writes', async () => {
    const storage = new MemoryStorage()
    const table = new DeltaTable(storage, 'test-table')
    
    console.log('Initial version:', await table.version())
    
    // First write
    const commit1 = await table.write([{ id: 1, value: 100 }])
    console.log('After first write, version:', await table.version())
    console.log('Files in _delta_log:', await storage.list('test-table/_delta_log'))
    
    // Second write
    const commit2 = await table.write([{ id: 2, value: 200 }])
    console.log('After second write, version:', await table.version())
    console.log('Files in _delta_log:', await storage.list('test-table/_delta_log'))
    
    expect(commit2.version).toBe(1)
  })
})
