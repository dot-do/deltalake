[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / isValidRemoveAction

# Function: isValidRemoveAction()

> **isValidRemoveAction**(`obj`): `obj is { path: string; deletionTimestamp: number; dataChange: boolean; partitionValues?: Record<string, string>; extendedFileMetadata?: boolean; size?: number }`

Defined in: src/delta/index.ts:3761

Type guard to validate RemoveAction inner structure from unknown input

## Parameters

### obj

`unknown`

## Returns

`obj is { path: string; deletionTimestamp: number; dataChange: boolean; partitionValues?: Record<string, string>; extendedFileMetadata?: boolean; size?: number }`
