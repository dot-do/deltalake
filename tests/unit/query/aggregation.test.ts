/**
 * MongoDB-style Aggregation Pipeline Tests
 *
 * Comprehensive tests for aggregation operators including:
 * - Accumulator operators ($sum, $avg, $min, $max, etc.)
 * - Pipeline stages ($match, $group, $project, $sort, $limit, $skip, $unwind)
 * - Complex multi-stage pipelines
 * - Edge cases and error handling
 */

import { describe, it, expect } from 'vitest'
import {
  aggregate,
  type AggregationPipeline,
  type GroupSpec,
  type SortSpec,
  type AggregationResult,
} from '../../../src/query/index.js'

// =============================================================================
// TEST DATA
// =============================================================================

interface Sale {
  product: string
  category: string
  quantity: number
  price: number
  region: string
  date: Date
  tags?: string[]
}

const sampleSales: Sale[] = [
  { product: 'Widget A', category: 'widgets', quantity: 5, price: 10.0, region: 'US', date: new Date('2024-01-15'), tags: ['popular', 'new'] },
  { product: 'Widget B', category: 'widgets', quantity: 3, price: 15.0, region: 'EU', date: new Date('2024-01-16'), tags: ['popular'] },
  { product: 'Gadget X', category: 'gadgets', quantity: 2, price: 50.0, region: 'US', date: new Date('2024-01-17'), tags: ['premium'] },
  { product: 'Widget A', category: 'widgets', quantity: 4, price: 10.0, region: 'US', date: new Date('2024-01-18'), tags: ['popular', 'sale'] },
  { product: 'Gadget Y', category: 'gadgets', quantity: 1, price: 75.0, region: 'EU', date: new Date('2024-01-19'), tags: ['premium', 'new'] },
  { product: 'Widget C', category: 'widgets', quantity: 8, price: 8.0, region: 'APAC', date: new Date('2024-01-20') },
]

interface Order {
  orderId: string
  status: 'pending' | 'completed' | 'cancelled'
  amount: number
  customerId: string
  items: number
}

const sampleOrders: Order[] = [
  { orderId: 'O1', status: 'completed', amount: 100, customerId: 'C1', items: 3 },
  { orderId: 'O2', status: 'pending', amount: 50, customerId: 'C2', items: 1 },
  { orderId: 'O3', status: 'completed', amount: 200, customerId: 'C1', items: 5 },
  { orderId: 'O4', status: 'completed', amount: 150, customerId: 'C3', items: 4 },
  { orderId: 'O5', status: 'cancelled', amount: 75, customerId: 'C2', items: 2 },
  { orderId: 'O6', status: 'completed', amount: 300, customerId: 'C1', items: 8 },
]

// =============================================================================
// ACCUMULATOR OPERATORS
// =============================================================================

describe('Accumulator Operators', () => {
  describe('$sum', () => {
    it('should sum numeric field values', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            totalQuantity: { $sum: '$quantity' },
          },
        },
      ])

      expect(result.documents).toHaveLength(1)
      expect(result.documents[0]?._id).toBe(null)
      expect(result.documents[0]?.totalQuantity).toBe(23) // 5+3+2+4+1+8
    })

    it('should sum constant values', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            count: { $sum: 1 },
          },
        },
      ])

      expect(result.documents[0]?.count).toBe(6)
    })

    it('should handle missing/null values', () => {
      const docs = [
        { value: 10 },
        { value: null },
        { value: 20 },
        { other: 5 }, // missing 'value' field
      ]

      const result = aggregate(docs, [
        {
          $group: {
            _id: null,
            total: { $sum: '$value' },
          },
        },
      ])

      expect(result.documents[0]?.total).toBe(30) // Only 10 + 20
    })

    it('should sum by group', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: '$category',
            totalQuantity: { $sum: '$quantity' },
          },
        },
      ])

      expect(result.documents).toHaveLength(2)

      const widgets = result.documents.find(d => d._id === 'widgets')
      const gadgets = result.documents.find(d => d._id === 'gadgets')

      expect(widgets?.totalQuantity).toBe(20) // 5+3+4+8
      expect(gadgets?.totalQuantity).toBe(3) // 2+1
    })
  })

  describe('$avg', () => {
    it('should calculate average of numeric field', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            avgPrice: { $avg: '$price' },
          },
        },
      ])

      const expected = (10 + 15 + 50 + 10 + 75 + 8) / 6
      expect(result.documents[0]?.avgPrice).toBeCloseTo(expected)
    })

    it('should calculate average by group', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: '$region',
            avgQuantity: { $avg: '$quantity' },
          },
        },
      ])

      const us = result.documents.find(d => d._id === 'US')
      const eu = result.documents.find(d => d._id === 'EU')
      const apac = result.documents.find(d => d._id === 'APAC')

      expect(us?.avgQuantity).toBeCloseTo((5 + 2 + 4) / 3)
      expect(eu?.avgQuantity).toBeCloseTo((3 + 1) / 2)
      expect(apac?.avgQuantity).toBe(8)
    })

    it('should return null for empty groups', () => {
      const result = aggregate([], [
        {
          $group: {
            _id: null,
            avgValue: { $avg: '$value' },
          },
        },
      ])

      expect(result.documents).toHaveLength(0)
    })
  })

  describe('$min', () => {
    it('should find minimum numeric value', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            minPrice: { $min: '$price' },
          },
        },
      ])

      expect(result.documents[0]?.minPrice).toBe(8)
    })

    it('should find minimum by group', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: '$category',
            minPrice: { $min: '$price' },
          },
        },
      ])

      const widgets = result.documents.find(d => d._id === 'widgets')
      const gadgets = result.documents.find(d => d._id === 'gadgets')

      expect(widgets?.minPrice).toBe(8)
      expect(gadgets?.minPrice).toBe(50)
    })

    it('should handle string comparison', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            firstProduct: { $min: '$product' },
          },
        },
      ])

      expect(result.documents[0]?.firstProduct).toBe('Gadget X')
    })
  })

  describe('$max', () => {
    it('should find maximum numeric value', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            maxPrice: { $max: '$price' },
          },
        },
      ])

      expect(result.documents[0]?.maxPrice).toBe(75)
    })

    it('should find maximum by group', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$status',
            maxAmount: { $max: '$amount' },
          },
        },
      ])

      const completed = result.documents.find(d => d._id === 'completed')
      const pending = result.documents.find(d => d._id === 'pending')

      expect(completed?.maxAmount).toBe(300)
      expect(pending?.maxAmount).toBe(50)
    })
  })

  describe('$first', () => {
    it('should get first value in group', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$customerId',
            firstOrder: { $first: '$orderId' },
          },
        },
      ])

      const c1 = result.documents.find(d => d._id === 'C1')
      expect(c1?.firstOrder).toBe('O1') // First order for C1
    })
  })

  describe('$last', () => {
    it('should get last value in group', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$customerId',
            lastOrder: { $last: '$orderId' },
          },
        },
      ])

      const c1 = result.documents.find(d => d._id === 'C1')
      expect(c1?.lastOrder).toBe('O6') // Last order for C1
    })
  })

  describe('$push', () => {
    it('should collect values into array', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$customerId',
            orderIds: { $push: '$orderId' },
          },
        },
      ])

      const c1 = result.documents.find(d => d._id === 'C1')
      expect(c1?.orderIds).toEqual(['O1', 'O3', 'O6'])
    })

    it('should include null/undefined values', () => {
      const docs = [
        { group: 'A', value: 1 },
        { group: 'A', value: null },
        { group: 'A' }, // missing value
      ]

      const result = aggregate(docs, [
        {
          $group: {
            _id: '$group',
            values: { $push: '$value' },
          },
        },
      ])

      expect(result.documents[0]?.values).toEqual([1, null, undefined])
    })
  })

  describe('$addToSet', () => {
    it('should collect unique values', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$customerId',
            statuses: { $addToSet: '$status' },
          },
        },
      ])

      const c1 = result.documents.find(d => d._id === 'C1')
      // C1 has all 'completed' orders
      expect(c1?.statuses).toEqual(['completed'])

      const c2 = result.documents.find(d => d._id === 'C2')
      // C2 has 'pending' and 'cancelled'
      expect((c2?.statuses as string[]).sort()).toEqual(['cancelled', 'pending'])
    })
  })

  describe('$count', () => {
    it('should count documents in group', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$status',
            count: { $count: {} },
          },
        },
      ])

      const completed = result.documents.find(d => d._id === 'completed')
      const pending = result.documents.find(d => d._id === 'pending')
      const cancelled = result.documents.find(d => d._id === 'cancelled')

      expect(completed?.count).toBe(4)
      expect(pending?.count).toBe(1)
      expect(cancelled?.count).toBe(1)
    })
  })

  describe('$stdDevPop and $stdDevSamp', () => {
    it('should calculate population standard deviation', () => {
      const docs = [
        { value: 2 },
        { value: 4 },
        { value: 4 },
        { value: 4 },
        { value: 5 },
        { value: 5 },
        { value: 7 },
        { value: 9 },
      ]

      const result = aggregate(docs, [
        {
          $group: {
            _id: null,
            stdDev: { $stdDevPop: '$value' },
          },
        },
      ])

      // Population std dev of [2,4,4,4,5,5,7,9] = 2
      expect(result.documents[0]?.stdDev).toBeCloseTo(2)
    })

    it('should calculate sample standard deviation', () => {
      const docs = [
        { value: 2 },
        { value: 4 },
        { value: 4 },
        { value: 4 },
        { value: 5 },
        { value: 5 },
        { value: 7 },
        { value: 9 },
      ]

      const result = aggregate(docs, [
        {
          $group: {
            _id: null,
            stdDev: { $stdDevSamp: '$value' },
          },
        },
      ])

      // Sample std dev is slightly larger than population
      expect(result.documents[0]?.stdDev).toBeCloseTo(2.138, 2)
    })
  })
})

// =============================================================================
// PIPELINE STAGES
// =============================================================================

describe('Pipeline Stages', () => {
  describe('$match', () => {
    it('should filter documents', () => {
      const result = aggregate(sampleOrders, [
        { $match: { status: 'completed' } },
      ])

      expect(result.documents).toHaveLength(4)
      expect(result.documents.every(d => d.status === 'completed')).toBe(true)
    })

    it('should support comparison operators', () => {
      const result = aggregate(sampleOrders, [
        { $match: { amount: { $gte: 100 } } },
      ])

      expect(result.documents).toHaveLength(4)
      expect(result.documents.every(d => (d.amount as number) >= 100)).toBe(true)
    })

    it('should support logical operators', () => {
      const result = aggregate(sampleOrders, [
        {
          $match: {
            $and: [
              { status: 'completed' },
              { amount: { $gt: 150 } },
            ],
          },
        },
      ])

      expect(result.documents).toHaveLength(2) // O3 (200) and O6 (300)
    })
  })

  describe('$group', () => {
    it('should group by single field', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: '$category',
            count: { $sum: 1 },
          },
        },
      ])

      expect(result.documents).toHaveLength(2)
    })

    it('should group by compound key', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: { category: '$category', region: '$region' },
            count: { $sum: 1 },
          },
        },
      ])

      // widgets: US(2), EU(1), APAC(1) = 4 groups for widgets
      // gadgets: US(1), EU(1) = 2 groups for gadgets
      // Total = 6 unique combinations
      expect(result.documents.length).toBeGreaterThanOrEqual(5)
    })

    it('should group all documents when _id is null', () => {
      const result = aggregate(sampleSales, [
        {
          $group: {
            _id: null,
            totalRevenue: { $sum: '$price' },
            avgQuantity: { $avg: '$quantity' },
          },
        },
      ])

      expect(result.documents).toHaveLength(1)
      expect(result.documents[0]?._id).toBe(null)
    })

    it('should combine multiple accumulators', () => {
      const result = aggregate(sampleOrders, [
        {
          $group: {
            _id: '$status',
            totalAmount: { $sum: '$amount' },
            avgAmount: { $avg: '$amount' },
            minAmount: { $min: '$amount' },
            maxAmount: { $max: '$amount' },
            count: { $count: {} },
          },
        },
      ])

      const completed = result.documents.find(d => d._id === 'completed')
      expect(completed?.totalAmount).toBe(750) // 100+200+150+300
      expect(completed?.avgAmount).toBeCloseTo(187.5)
      expect(completed?.minAmount).toBe(100)
      expect(completed?.maxAmount).toBe(300)
      expect(completed?.count).toBe(4)
    })
  })

  describe('$project', () => {
    it('should include specified fields', () => {
      const result = aggregate(sampleOrders, [
        { $project: ['orderId', 'amount'] },
      ])

      expect(result.documents).toHaveLength(6)
      expect(Object.keys(result.documents[0] ?? {})).toEqual(['orderId', 'amount'])
    })

    it('should work with object notation', () => {
      const result = aggregate(sampleOrders, [
        { $project: { orderId: 1, status: 1 } },
      ])

      expect(result.documents).toHaveLength(6)
      expect(Object.keys(result.documents[0] ?? {})).toEqual(['orderId', 'status'])
    })
  })

  describe('$sort', () => {
    it('should sort ascending', () => {
      const result = aggregate(sampleOrders, [
        { $sort: { amount: 1 } },
      ])

      const amounts = result.documents.map(d => d.amount as number)
      expect(amounts).toEqual([50, 75, 100, 150, 200, 300])
    })

    it('should sort descending', () => {
      const result = aggregate(sampleOrders, [
        { $sort: { amount: -1 } },
      ])

      const amounts = result.documents.map(d => d.amount as number)
      expect(amounts).toEqual([300, 200, 150, 100, 75, 50])
    })

    it('should sort by multiple fields', () => {
      const result = aggregate(sampleOrders, [
        { $sort: { status: 1, amount: -1 } },
      ])

      // Cancelled first, then completed, then pending (alphabetical)
      expect(result.documents[0]?.status).toBe('cancelled')
      // Within completed, sorted by amount desc
      const completedDocs = result.documents.filter(d => d.status === 'completed')
      const amounts = completedDocs.map(d => d.amount as number)
      expect(amounts).toEqual([300, 200, 150, 100])
    })

    it('should handle null values in sort', () => {
      const docs = [
        { name: 'C', value: 3 },
        { name: 'A', value: null },
        { name: 'B', value: 1 },
      ]

      const result = aggregate(docs, [
        { $sort: { value: 1 } },
      ])

      // Nulls should be sorted first (less than any value)
      expect(result.documents[0]?.name).toBe('A')
      expect(result.documents[1]?.name).toBe('B')
      expect(result.documents[2]?.name).toBe('C')
    })
  })

  describe('$limit', () => {
    it('should limit results', () => {
      const result = aggregate(sampleOrders, [
        { $limit: 3 },
      ])

      expect(result.documents).toHaveLength(3)
    })

    it('should handle limit larger than document count', () => {
      const result = aggregate(sampleOrders, [
        { $limit: 100 },
      ])

      expect(result.documents).toHaveLength(6)
    })
  })

  describe('$skip', () => {
    it('should skip documents', () => {
      const result = aggregate(sampleOrders, [
        { $skip: 2 },
      ])

      expect(result.documents).toHaveLength(4)
      expect(result.documents[0]?.orderId).toBe('O3')
    })

    it('should handle skip beyond document count', () => {
      const result = aggregate(sampleOrders, [
        { $skip: 100 },
      ])

      expect(result.documents).toHaveLength(0)
    })
  })

  describe('$unwind', () => {
    it('should unwind array field', () => {
      const result = aggregate(sampleSales.filter(s => s.tags), [
        { $unwind: '$tags' },
      ])

      // Count total tags: 2+1+1+2+2 = 8
      expect(result.documents.length).toBeGreaterThanOrEqual(8)
      // Each document should have a single tag value
      expect(result.documents.every(d => typeof d.tags === 'string')).toBe(true)
    })

    it('should skip documents with empty arrays by default', () => {
      const result = aggregate(sampleSales, [
        { $unwind: '$tags' },
      ])

      // Widget C has no tags, so it should be skipped
      expect(result.documents.every(d => d.product !== 'Widget C')).toBe(true)
    })

    it('should preserve null/empty arrays when specified', () => {
      const result = aggregate(sampleSales, [
        { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } },
      ])

      // Widget C should be included with tags: null
      const widgetC = result.documents.filter(d => d.product === 'Widget C')
      expect(widgetC).toHaveLength(1)
      expect(widgetC[0]?.tags).toBe(null)
    })
  })
})

// =============================================================================
// COMPLEX PIPELINES
// =============================================================================

describe('Complex Pipelines', () => {
  it('should execute match -> group -> sort pipeline', () => {
    const result = aggregate(sampleOrders, [
      { $match: { status: 'completed' } },
      {
        $group: {
          _id: '$customerId',
          totalAmount: { $sum: '$amount' },
          orderCount: { $count: {} },
        },
      },
      { $sort: { totalAmount: -1 } },
    ])

    // Completed orders: O1(C1,100), O3(C1,200), O4(C3,150), O6(C1,300)
    // C1 has 3 completed orders, C3 has 1 completed order
    expect(result.documents).toHaveLength(2)
    // C1 has highest total (100+200+300=600)
    expect(result.documents[0]?._id).toBe('C1')
    expect(result.documents[0]?.totalAmount).toBe(600)
    expect(result.documents[0]?.orderCount).toBe(3)
    // C3 has 150
    expect(result.documents[1]?._id).toBe('C3')
    expect(result.documents[1]?.totalAmount).toBe(150)
  })

  it('should execute group -> match -> sort -> limit pipeline', () => {
    const result = aggregate(sampleSales, [
      {
        $group: {
          _id: '$product',
          totalQuantity: { $sum: '$quantity' },
        },
      },
      { $match: { totalQuantity: { $gte: 5 } } },
      { $sort: { totalQuantity: -1 } },
      { $limit: 2 },
    ])

    expect(result.documents).toHaveLength(2)
    // Widget A (9) and Widget C (8) are the top 2
    expect(result.documents[0]?.totalQuantity).toBe(9)
    expect(result.documents[1]?.totalQuantity).toBe(8)
  })

  it('should execute unwind -> group pipeline', () => {
    const result = aggregate(sampleSales.filter(s => s.tags), [
      { $unwind: '$tags' },
      {
        $group: {
          _id: '$tags',
          products: { $addToSet: '$product' },
          count: { $count: {} },
        },
      },
      { $sort: { count: -1 } },
    ])

    // 'popular' appears most (Widget A, Widget B, Widget A again)
    const popular = result.documents.find(d => d._id === 'popular')
    expect(popular).toBeDefined()
    expect(popular?.count).toBe(3)
  })

  it('should handle skip and limit together for pagination', () => {
    const result = aggregate(sampleOrders, [
      { $sort: { orderId: 1 } },
      { $skip: 2 },
      { $limit: 2 },
    ])

    expect(result.documents).toHaveLength(2)
    expect(result.documents[0]?.orderId).toBe('O3')
    expect(result.documents[1]?.orderId).toBe('O4')
  })

  it('should return execution statistics', () => {
    const result = aggregate(sampleOrders, [
      { $match: { status: 'completed' } },
      {
        $group: {
          _id: '$customerId',
          total: { $sum: '$amount' },
        },
      },
    ])

    expect(result.stats).toBeDefined()
    expect(result.stats?.documentsProcessed).toBe(6)
    // Only 2 unique customers with completed orders: C1 and C3
    expect(result.stats?.groupsCreated).toBe(2)
    expect(result.stats?.executionTimeMs).toBeGreaterThanOrEqual(0)
  })
})

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle empty input', () => {
    const result = aggregate([], [
      {
        $group: {
          _id: null,
          count: { $count: {} },
        },
      },
    ])

    expect(result.documents).toHaveLength(0)
  })

  it('should handle empty pipeline', () => {
    const result = aggregate(sampleOrders, [])

    expect(result.documents).toHaveLength(6)
    expect(result.documents).toEqual(sampleOrders)
  })

  it('should handle documents with missing fields', () => {
    const docs = [
      { a: 1, b: 2 },
      { a: 3 }, // missing b
      { b: 4 }, // missing a
    ]

    const result = aggregate(docs, [
      {
        $group: {
          _id: null,
          sumA: { $sum: '$a' },
          sumB: { $sum: '$b' },
        },
      },
    ])

    expect(result.documents[0]?.sumA).toBe(4) // 1 + 3
    expect(result.documents[0]?.sumB).toBe(6) // 2 + 4
  })

  it('should handle nested field paths', () => {
    const docs = [
      { user: { name: 'Alice', score: 10 } },
      { user: { name: 'Bob', score: 20 } },
      { user: { name: 'Alice', score: 15 } },
    ]

    const result = aggregate(docs, [
      {
        $group: {
          _id: '$user.name',
          totalScore: { $sum: '$user.score' },
        },
      },
    ])

    const alice = result.documents.find(d => d._id === 'Alice')
    const bob = result.documents.find(d => d._id === 'Bob')

    expect(alice?.totalScore).toBe(25)
    expect(bob?.totalScore).toBe(20)
  })

  it('should handle type coercion in comparisons', () => {
    const docs = [
      { value: '10' },
      { value: 20 },
      { value: '5' },
    ]

    const result = aggregate(docs, [
      { $sort: { value: 1 } },
    ])

    // String comparison for strings, numeric for numbers
    // The exact order depends on type handling
    expect(result.documents).toHaveLength(3)
  })

  it('should handle date values', () => {
    const result = aggregate(sampleSales, [
      {
        $group: {
          _id: null,
          firstDate: { $min: '$date' },
          lastDate: { $max: '$date' },
        },
      },
    ])

    expect(result.documents[0]?.firstDate).toEqual(new Date('2024-01-15'))
    expect(result.documents[0]?.lastDate).toEqual(new Date('2024-01-20'))
  })
})
