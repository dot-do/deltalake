[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCConsumer

# Class: CDCConsumer\<T\>

Defined in: src/cdc/index.ts:376

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

Defined in: src/cdc/index.ts:382

#### Parameters

##### options

[`CDCConsumerOptions`](../interfaces/CDCConsumerOptions.md) = `{}`

#### Returns

`CDCConsumer`\<`T`\>

## Methods

### subscribe()

> **subscribe**(`handler`): () => `void`

Defined in: src/cdc/index.ts:396

Subscribe to CDC records.

#### Parameters

##### handler

[`CDCRecordHandler`](../type-aliases/CDCRecordHandler.md)\<`T`\>

#### Returns

Unsubscribe function

> (): `void`

##### Returns

`void`

***

### process()

> **process**(`record`): `Promise`\<`void`\>

Defined in: src/cdc/index.ts:410

Process a record, applying filters and notifying handlers.
Updates position after successful processing.

#### Parameters

##### record

[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>

#### Returns

`Promise`\<`void`\>

***

### seekTo()

> **seekTo**(`seq`): `void`

Defined in: src/cdc/index.ts:437

Seek to a specific sequence position.

#### Parameters

##### seq

`bigint`

#### Returns

`void`

***

### seekToTimestamp()

> **seekToTimestamp**(`ts`): `void`

Defined in: src/cdc/index.ts:444

Set timestamp filter for future records.

#### Parameters

##### ts

`Date`

#### Returns

`void`

***

### getPosition()

> **getPosition**(): `bigint`

Defined in: src/cdc/index.ts:451

Get current sequence position.

#### Returns

`bigint`

***

### getSubscriberCount()

> **getSubscriberCount**(): `number`

Defined in: src/cdc/index.ts:458

Get number of active subscribers.

#### Returns

`number`
