[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / isValidCommitInfoAction

# Function: isValidCommitInfoAction()

> **isValidCommitInfoAction**(`obj`): `obj is { timestamp: number; operation: string; operationParameters?: Record<string, string>; readVersion?: number; isolationLevel?: string; isBlindAppend?: boolean }`

Defined in: src/delta/index.ts:3808

Type guard to validate CommitInfoAction inner structure from unknown input

## Parameters

### obj

`unknown`

## Returns

`obj is { timestamp: number; operation: string; operationParameters?: Record<string, string>; readVersion?: number; isolationLevel?: string; isBlindAppend?: boolean }`
