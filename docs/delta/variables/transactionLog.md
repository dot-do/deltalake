[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [delta](../README.md) / transactionLog

# Variable: transactionLog

> `const` **transactionLog**: `object`

Defined in: [src/delta/index.ts:656](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/delta/index.ts#L656)

Transaction log utilities for serialization, parsing, and validation

## Type Declaration

### serializeAction()

> **serializeAction**(`action`): `string`

Serialize a single action to JSON

#### Parameters

##### action

[`DeltaAction`](../type-aliases/DeltaAction.md)

#### Returns

`string`

### parseAction()

> **parseAction**(`json`): [`DeltaAction`](../type-aliases/DeltaAction.md)

Parse a single action from JSON

#### Parameters

##### json

`string`

JSON string containing a single Delta action

#### Returns

[`DeltaAction`](../type-aliases/DeltaAction.md)

Parsed DeltaAction object

#### Throws

Error if JSON is empty, invalid, or missing a recognized action type

### serializeCommit()

> **serializeCommit**(`actions`): `string`

Serialize multiple actions to NDJSON format

#### Parameters

##### actions

[`DeltaAction`](../type-aliases/DeltaAction.md)[]

#### Returns

`string`

### parseCommit()

> **parseCommit**(`content`): [`DeltaAction`](../type-aliases/DeltaAction.md)[]

Parse NDJSON content to actions

#### Parameters

##### content

`string`

#### Returns

[`DeltaAction`](../type-aliases/DeltaAction.md)[]

### formatVersion()

> **formatVersion**(`version`): `string`

Format version number as 20-digit zero-padded string

#### Parameters

##### version

Version number (supports both number and bigint)

`number` | `bigint`

#### Returns

`string`

Zero-padded version string

#### Throws

Error if version is negative or exceeds maximum digits

#### See

formatVersion - The standalone function this delegates to

### parseVersionFromFilename()

> **parseVersionFromFilename**(`filename`): `number`

Parse version number from filename

#### Parameters

##### filename

`string`

Log filename (can include path)

#### Returns

`number`

Parsed version number

#### Throws

Error if filename doesn't match expected format

#### See

parseVersionFromFilenameUtil - The shared function this delegates to

### getLogFilePath()

> **getLogFilePath**(`tablePath`, `version`): `string`

Get the log file path for a version

#### Parameters

##### tablePath

`string`

Base path of the Delta table

##### version

`number`

Version number

#### Returns

`string`

Full path to the log file

#### See

getLogFilePathUtil - The shared function this delegates to

### validateAction()

> **validateAction**(`action`): `object`

Validate an action structure

#### Parameters

##### action

[`DeltaAction`](../type-aliases/DeltaAction.md)

#### Returns

`object`

##### valid

> **valid**: `boolean`

##### errors

> **errors**: `string`[]
