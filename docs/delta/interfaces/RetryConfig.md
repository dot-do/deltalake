[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / RetryConfig

# Interface: RetryConfig

Defined in: [src/delta/retry.ts:77](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L77)

Configuration options for retry behavior

## Properties

### maxRetries?

> `readonly` `optional` **maxRetries**: `number`

Defined in: [src/delta/retry.ts:79](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L79)

Maximum number of retry attempts (default: 3)

***

### baseDelay?

> `readonly` `optional` **baseDelay**: `number`

Defined in: [src/delta/retry.ts:81](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L81)

Base delay in milliseconds (default: 100)

***

### maxDelay?

> `readonly` `optional` **maxDelay**: `number`

Defined in: [src/delta/retry.ts:83](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L83)

Maximum delay in milliseconds (default: 10000)

***

### multiplier?

> `readonly` `optional` **multiplier**: `number`

Defined in: [src/delta/retry.ts:85](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L85)

Multiplier for exponential backoff (default: 2)

***

### jitter?

> `readonly` `optional` **jitter**: `boolean`

Defined in: [src/delta/retry.ts:87](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L87)

Whether to add random jitter to delays (default: true)

***

### jitterFactor?

> `readonly` `optional` **jitterFactor**: `number`

Defined in: [src/delta/retry.ts:89](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L89)

Jitter factor (default: 0.5, meaning +/- 50%)

***

### isRetryable()?

> `readonly` `optional` **isRetryable**: (`error`) => `boolean`

Defined in: [src/delta/retry.ts:91](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L91)

Custom predicate to determine if error is retryable

#### Parameters

##### error

`Error`

#### Returns

`boolean`

***

### onRetry()?

> `readonly` `optional` **onRetry**: (`info`) => `boolean` \| `void`

Defined in: [src/delta/retry.ts:93](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L93)

Called before each retry attempt. Return false to abort retries.

#### Parameters

##### info

[`RetryInfo`](RetryInfo.md)

#### Returns

`boolean` \| `void`

***

### onSuccess()?

> `readonly` `optional` **onSuccess**: (`info`) => `void`

Defined in: [src/delta/retry.ts:95](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L95)

Called on successful completion

#### Parameters

##### info

[`SuccessInfo`](SuccessInfo.md)

#### Returns

`void`

***

### onFailure()?

> `readonly` `optional` **onFailure**: (`info`) => `void`

Defined in: [src/delta/retry.ts:97](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L97)

Called when all retries are exhausted

#### Parameters

##### info

[`FailureInfo`](FailureInfo.md)

#### Returns

`void`

***

### returnMetrics?

> `readonly` `optional` **returnMetrics**: `boolean`

Defined in: [src/delta/retry.ts:99](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L99)

Whether to return metrics with result

***

### signal?

> `readonly` `optional` **signal**: `AbortSignal`

Defined in: [src/delta/retry.ts:101](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/retry.ts#L101)

AbortController signal to cancel retries
