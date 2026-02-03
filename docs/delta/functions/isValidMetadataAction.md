[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / isValidMetadataAction

# Function: isValidMetadataAction()

> **isValidMetadataAction**(`obj`): `obj is { id: string; name?: string; description?: string; format: { provider: string; options?: Record<string, string> }; schemaString: string; partitionColumns: string[]; configuration?: Record<string, string>; createdTime?: number }`

Defined in: src/delta/index.ts:3777

Type guard to validate MetadataAction inner structure from unknown input

## Parameters

### obj

`unknown`

## Returns

`obj is { id: string; name?: string; description?: string; format: { provider: string; options?: Record<string, string> }; schemaString: string; partitionColumns: string[]; configuration?: Record<string, string>; createdTime?: number }`
