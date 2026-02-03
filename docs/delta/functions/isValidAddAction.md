[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / isValidAddAction

# Function: isValidAddAction()

> **isValidAddAction**(`obj`): `obj is { path: string; size: number; modificationTime: number; dataChange: boolean; partitionValues?: Record<string, string>; stats?: string; tags?: Record<string, string>; deletionVector?: DeletionVectorDescriptor }`

Defined in: src/delta/index.ts:3744

Type guard to validate AddAction inner structure from unknown input

## Parameters

### obj

`unknown`

## Returns

`obj is { path: string; size: number; modificationTime: number; dataChange: boolean; partitionValues?: Record<string, string>; stats?: string; tags?: Record<string, string>; deletionVector?: DeletionVectorDescriptor }`
