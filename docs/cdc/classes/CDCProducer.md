[**@dotdo/deltalake v0.0.1**](../../README.md)

***

[@dotdo/deltalake](../../modules.md) / [cdc](../README.md) / CDCProducer

# Class: CDCProducer\<T\>

Defined in: [src/cdc/index.ts:305](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L305)

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

Defined in: [src/cdc/index.ts:309](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L309)

#### Parameters

##### options

[`CDCProducerOptions`](../interfaces/CDCProducerOptions.md)

#### Returns

`CDCProducer`\<`T`\>

## Methods

### getSequence()

> **getSequence**(): `bigint`

Defined in: [src/cdc/index.ts:319](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L319)

Get the current sequence number (useful for checkpointing).

#### Returns

`bigint`

***

### resetSequence()

> **resetSequence**(`seq`): `void`

Defined in: [src/cdc/index.ts:326](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L326)

Reset the sequence number (use with caution).

#### Parameters

##### seq

`bigint` = `0n`

#### Returns

`void`

***

### emit()

> **emit**(`op`, `id`, `before`, `after`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: [src/cdc/index.ts:333](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L333)

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

Defined in: [src/cdc/index.ts:376](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L376)

Emit a create record (operation: 'c').

#### Parameters

##### id

`string`

Entity identifier

##### data

`T`

The new entity data

##### txn?

`string`

Optional transaction ID for exactly-once semantics

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

The generated CDC record

#### Example

```typescript
const record = await producer.create('user-1', {
  name: 'Alice',
  email: 'alice@example.com'
})
// record._op === 'c'
// record._before === null
// record._after === { name: 'Alice', email: 'alice@example.com' }
```

***

### update()

> **update**(`id`, `before`, `after`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: [src/cdc/index.ts:401](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L401)

Emit an update record (operation: 'u').

#### Parameters

##### id

`string`

Entity identifier

##### before

`T`

The entity state before the update

##### after

`T`

The entity state after the update

##### txn?

`string`

Optional transaction ID for exactly-once semantics

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

The generated CDC record

#### Example

```typescript
const record = await producer.update(
  'user-1',
  { name: 'Alice', score: 100 },
  { name: 'Alice', score: 150 }
)
// record._op === 'u'
// record._before === { name: 'Alice', score: 100 }
// record._after === { name: 'Alice', score: 150 }
```

***

### delete()

> **delete**(`id`, `before`, `txn?`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

Defined in: [src/cdc/index.ts:424](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L424)

Emit a delete record (operation: 'd').

#### Parameters

##### id

`string`

Entity identifier

##### before

`T`

The entity state before deletion

##### txn?

`string`

Optional transaction ID for exactly-once semantics

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>\>

The generated CDC record

#### Example

```typescript
const record = await producer.delete('user-1', {
  name: 'Alice',
  email: 'alice@example.com'
})
// record._op === 'd'
// record._before === { name: 'Alice', email: 'alice@example.com' }
// record._after === null
```

***

### snapshot()

> **snapshot**(`records`): `Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>[]\>

Defined in: [src/cdc/index.ts:448](https://github.com/dot-do/deltalake/blob/d874c146f352ad9fbb34fe5d2e0ac828849a01ca/src/cdc/index.ts#L448)

Emit snapshot records (bulk 'r' operations).

Useful for initial data sync or point-in-time snapshots.
Each record represents the current state of an entity.

#### Parameters

##### records

readonly `object`[]

Array of entities to snapshot

#### Returns

`Promise`\<[`CDCRecord`](../interfaces/CDCRecord.md)\<`T`\>[]\>

Array of generated CDC records

#### Example

```typescript
const records = await producer.snapshot([
  { id: 'user-1', data: { name: 'Alice' } },
  { id: 'user-2', data: { name: 'Bob' } }
])
// records[0]._op === 'r'
// records[0]._before === null
// records[0]._after === { name: 'Alice' }
```
