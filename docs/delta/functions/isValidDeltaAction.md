[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / isValidDeltaAction

# Function: isValidDeltaAction()

> **isValidDeltaAction**(`obj`): `obj is DeltaAction`

Defined in: src/delta/index.ts:3825

Type guard to validate a complete DeltaAction from unknown input (e.g., JSON.parse result)
This validates both the wrapper structure and inner action data

## Parameters

### obj

`unknown`

## Returns

`obj is DeltaAction`
