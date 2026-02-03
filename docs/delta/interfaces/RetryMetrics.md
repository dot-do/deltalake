[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / RetryMetrics

# Interface: RetryMetrics

Defined in: src/delta/retry.ts:57

Metrics collected during retry execution

## Properties

### attempts

> **attempts**: `number`

Defined in: src/delta/retry.ts:59

Total number of attempts made

***

### retries

> **retries**: `number`

Defined in: src/delta/retry.ts:61

Number of retries (attempts - 1)

***

### succeeded

> **succeeded**: `boolean`

Defined in: src/delta/retry.ts:63

Whether the operation succeeded

***

### totalDelayMs

> **totalDelayMs**: `number`

Defined in: src/delta/retry.ts:65

Total delay time in milliseconds

***

### elapsedMs

> **elapsedMs**: `number`

Defined in: src/delta/retry.ts:67

Total elapsed time in milliseconds

***

### delays

> **delays**: `number`[]

Defined in: src/delta/retry.ts:69

Array of individual delay times

***

### errors

> **errors**: `Error`[]

Defined in: src/delta/retry.ts:71

Array of errors encountered
