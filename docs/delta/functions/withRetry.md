[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / withRetry

# Function: withRetry()

## Call Signature

> **withRetry**\<`T`\>(`fn`, `config?`): `Promise`\<[`RetryResultWithMetrics`](../interfaces/RetryResultWithMetrics.md)\<`T`\>\>

Defined in: src/delta/retry.ts:243

Wrap a function with retry logic using exponential backoff

### Type Parameters

#### T

`T`

### Parameters

#### fn

() => `T` \| `Promise`\<`T`\>

The async function to retry

#### config?

[`RetryConfig`](../interfaces/RetryConfig.md) & `object`

Retry configuration options

### Returns

`Promise`\<[`RetryResultWithMetrics`](../interfaces/RetryResultWithMetrics.md)\<`T`\>\>

The result of the function, or throws after all retries exhausted

### Example

```typescript
// Basic usage
const result = await withRetry(async () => {
  return await table.write(rows)
})

// With custom config
const result = await withRetry(async () => {
  return await table.write(rows)
}, {
  maxRetries: 5,
  baseDelay: 200,
  onRetry: ({ attempt, error }) => {
    console.log(`Retry ${attempt} after error: ${error.message}`)
  }
})

// With metrics
const { result, metrics } = await withRetry(async () => {
  return await table.write(rows)
}, {
  returnMetrics: true
})
console.log(`Succeeded after ${metrics.attempts} attempts`)
```

## Call Signature

> **withRetry**\<`T`\>(`fn`, `config?`): `Promise`\<`T`\>

Defined in: src/delta/retry.ts:248

Wrap a function with retry logic using exponential backoff

### Type Parameters

#### T

`T`

### Parameters

#### fn

() => `T` \| `Promise`\<`T`\>

The async function to retry

#### config?

[`RetryConfig`](../interfaces/RetryConfig.md) & `object`

Retry configuration options

### Returns

`Promise`\<`T`\>

The result of the function, or throws after all retries exhausted

### Example

```typescript
// Basic usage
const result = await withRetry(async () => {
  return await table.write(rows)
})

// With custom config
const result = await withRetry(async () => {
  return await table.write(rows)
}, {
  maxRetries: 5,
  baseDelay: 200,
  onRetry: ({ attempt, error }) => {
    console.log(`Retry ${attempt} after error: ${error.message}`)
  }
})

// With metrics
const { result, metrics } = await withRetry(async () => {
  return await table.write(rows)
}, {
  returnMetrics: true
})
console.log(`Succeeded after ${metrics.attempts} attempts`)
```
