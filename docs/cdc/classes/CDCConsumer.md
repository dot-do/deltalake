[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConsumer

# Class: CDCConsumer\<T\>

Defined in: [src/cdc/index.ts:504](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L504)

CDC Consumer for processing CDC records.
Supports filtering, position tracking, and multiple subscribers.

## Example

```typescript
const consumer = new CDCConsumer<User>({
  fromSeq: 100n,
  operations: ['c', 'u'] // Only creates and updates
})

consumer.subscribe(async (record) => {
  console.log(`${record._op}: ${record._id}`)
})

await consumer.process(record)
```

## Type Parameters

### T

`T` = `unknown`

The data type being consumed

## Constructors

### Constructor

> **new CDCConsumer**\<`T`\>(`options`): `CDCConsumer`\<`T`\>

Defined in: [src/cdc/index.ts:510](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L510)

#### Parameters

##### options

[`CDCConsumerOptions`](../interfaces/CDCConsumerOptions.md) = `{}`

#### Returns

`CDCConsumer`\<`T`\>

## Methods

### subscribe()

> **subscribe**(`handler`): () => `void`

Defined in: [src/cdc/index.ts:544](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L544)

Subscribe to CDC records.

Multiple handlers can be subscribed. Each handler receives all records
that pass the consumer's filters (operations, sequence, timestamp).

#### Parameters

##### handler

[`CDCRecordHandler`](../type-aliases/CDCRecordHandler.md)\<`T`\>

Async function to process each CDC record

#### Returns

Unsubscribe function to remove the handler

> (): `void`

##### Returns

`void`

#### Example

```typescript
// Subscribe to process records
const unsubscribe = consumer.subscribe(async (record) => {
  if (record._op === 'c') {
    await indexService.add(record._after)
  } else if (record._op === 'd') {
    await indexService.remove(record._id)
  }
})

// Later, unsubscribe when done
unsubscribe()
```

***

### process()

> **process**(`record`): `Promise`\<`void`\>

Defined in: [src/cdc/index.ts:577](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L577)

Process a record, applying filters and notifying handlers.

The record is only processed if it passes all configured filters:
- Operation type filter (if `operations` was specified)
- Sequence filter (record._seq must be >= current position)
- Timestamp filter (if `fromTimestamp` was specified)

After successful processing, the position is updated to record._seq + 1.

#### Parameters

##### record

[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>

The CDC record to process

#### Returns

`Promise`\<`void`\>

#### Example

```typescript
// Process records from a stream
for await (const record of cdcStream) {
  await consumer.process(record)
}

// Check current position after processing
console.log(`Processed up to sequence ${consumer.getPosition()}`)
```

***

### seekTo()

> **seekTo**(`seq`): `void`

Defined in: [src/cdc/index.ts:621](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L621)

Seek to a specific sequence position.

Records with sequence numbers less than this position will be skipped.
Use this to resume processing from a saved checkpoint.

#### Parameters

##### seq

`bigint`

The sequence number to seek to

#### Returns

`void`

#### Example

```typescript
// Resume from a saved checkpoint
const checkpoint = await loadCheckpoint()
consumer.seekTo(checkpoint.lastSeq)

// Process new records from the checkpoint position
for await (const record of cdcStream) {
  await consumer.process(record)
}
```

***

### seekToTimestamp()

> **seekToTimestamp**(`ts`): `void`

Defined in: [src/cdc/index.ts:640](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L640)

Set timestamp filter for future records.

Records with timestamps before this date will be skipped.
The timestamp is compared against the record's `_ts` field.

#### Parameters

##### ts

`Date`

The timestamp to filter from

#### Returns

`void`

#### Example

```typescript
// Only process records from the last hour
const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000)
consumer.seekToTimestamp(oneHourAgo)
```

***

### getPosition()

> **getPosition**(): `bigint`

Defined in: [src/cdc/index.ts:658](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L658)

Get current sequence position.

Returns the next sequence number that will be processed.
Useful for checkpointing consumer progress.

#### Returns

`bigint`

The current sequence position

#### Example

```typescript
// Save checkpoint after processing
await saveCheckpoint({ lastSeq: consumer.getPosition() })
```

***

### getSubscriberCount()

> **getSubscriberCount**(): `number`

Defined in: [src/cdc/index.ts:672](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L672)

Get number of active subscribers.

#### Returns

`number`

The count of subscribed handlers

#### Example

```typescript
console.log(`Active subscribers: ${consumer.getSubscriberCount()}`)
```
