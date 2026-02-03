[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCProducer

# Class: CDCProducer\<T\>

Defined in: src/cdc/index.ts:249

CDC Producer for generating standardized CDC records.
Used for converting data changes into CDC format for downstream consumers.

## Example

```typescript
const producer = new CDCProducer<User>({
  source: { database: 'mydb', collection: 'users' },
  system: 'mongolake'
})

const record = await producer.create('user-1', { name: 'Alice' })
```

## Type Parameters

### T

`T` = `unknown`

The data type being tracked

## Constructors

### Constructor

> **new CDCProducer**\<`T`\>(`options`): `CDCProducer`\<`T`\>

Defined in: src/cdc/index.ts:253

#### Parameters

##### options

[`CDCProducerOptions`](../interfaces/CDCProducerOptions.md)

#### Returns

`CDCProducer`\<`T`\>

## Methods

### getSequence()

> **getSequence**(): `bigint`

Defined in: src/cdc/index.ts:263

Get the current sequence number (useful for checkpointing).

#### Returns

`bigint`

***

### resetSequence()

> **resetSequence**(`seq`): `void`

Defined in: src/cdc/index.ts:270

Reset the sequence number (use with caution).

#### Parameters

##### seq

`bigint` = `0n`

#### Returns

`void`

***

### emit()

> **emit**(`op`, `id`, `before`, `after`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: src/cdc/index.ts:277

Emit a CDC record with the specified operation type.

#### Parameters

##### op

[`CDCOperation`](../type-aliases/CDCOperation.md)

##### id

`string`

##### before

`T` | `null`

##### after

`T` | `null`

##### txn?

`string`

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

***

### create()

> **create**(`id`, `data`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: src/cdc/index.ts:304

Emit a create record (operation: 'c').

#### Parameters

##### id

`string`

##### data

`T`

##### txn?

`string`

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

***

### update()

> **update**(`id`, `before`, `after`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: src/cdc/index.ts:311

Emit an update record (operation: 'u').

#### Parameters

##### id

`string`

##### before

`T`

##### after

`T`

##### txn?

`string`

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

***

### delete()

> **delete**(`id`, `before`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: src/cdc/index.ts:318

Emit a delete record (operation: 'd').

#### Parameters

##### id

`string`

##### before

`T`

##### txn?

`string`

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

***

### snapshot()

> **snapshot**(`records`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>[]\>

Defined in: src/cdc/index.ts:326

Emit snapshot records (bulk 'r' operations).
Useful for initial data sync or point-in-time snapshots.

#### Parameters

##### records

readonly `object`[]

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>[]\>
