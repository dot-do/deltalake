[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / RetryConfig

# Interface: RetryConfig

Defined in: src/delta/retry.ts:77

Configuration options for retry behavior

## Properties

### maxRetries?

> `optional` **maxRetries**: `number`

Defined in: src/delta/retry.ts:79

Maximum number of retry attempts (default: 3)

***

### baseDelay?

> `optional` **baseDelay**: `number`

Defined in: src/delta/retry.ts:81

Base delay in milliseconds (default: 100)

***

### maxDelay?

> `optional` **maxDelay**: `number`

Defined in: src/delta/retry.ts:83

Maximum delay in milliseconds (default: 10000)

***

### multiplier?

> `optional` **multiplier**: `number`

Defined in: src/delta/retry.ts:85

Multiplier for exponential backoff (default: 2)

***

### jitter?

> `optional` **jitter**: `boolean`

Defined in: src/delta/retry.ts:87

Whether to add random jitter to delays (default: true)

***

### jitterFactor?

> `optional` **jitterFactor**: `number`

Defined in: src/delta/retry.ts:89

Jitter factor (default: 0.5, meaning +/- 50%)

***

### isRetryable()?

> `optional` **isRetryable**: (`error`) => `boolean`

Defined in: src/delta/retry.ts:91

Custom predicate to determine if error is retryable

#### Parameters

##### error

`Error`

#### Returns

`boolean`

***

### onRetry()?

> `optional` **onRetry**: (`info`) => `boolean` \| `void`

Defined in: src/delta/retry.ts:93

Called before each retry attempt. Return false to abort retries.

#### Parameters

##### info

[`RetryInfo`](RetryInfo.md)

#### Returns

`boolean` \| `void`

***

### onSuccess()?

> `optional` **onSuccess**: (`info`) => `void`

Defined in: src/delta/retry.ts:95

Called on successful completion

#### Parameters

##### info

[`SuccessInfo`](SuccessInfo.md)

#### Returns

`void`

***

### onFailure()?

> `optional` **onFailure**: (`info`) => `void`

Defined in: src/delta/retry.ts:97

Called when all retries are exhausted

#### Parameters

##### info

[`FailureInfo`](FailureInfo.md)

#### Returns

`void`

***

### returnMetrics?

> `optional` **returnMetrics**: `boolean`

Defined in: src/delta/retry.ts:99

Whether to return metrics with result

***

### signal?

> `optional` **signal**: `AbortSignal`

Defined in: src/delta/retry.ts:101

AbortController signal to cancel retries
