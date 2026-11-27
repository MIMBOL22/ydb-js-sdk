import * as assert from 'node:assert'
import { create } from '@bufbuild/protobuf'
import { OperationSchema, StatusIds_StatusCode } from '@ydbjs/api/operation'
import { UpdateOffsetsInTransactionResponseSchema } from '@ydbjs/api/topic'
import { test } from 'vitest'

import { TopicPartitionSession } from '../partition-session.js'
import { _update_offsets_in_transaction } from './_update_offsets_in_transaction.js'
import {
	type TopicTxReadOffsets,
	collectTxReadOffsetUpdates,
	topicPartitionOffsetKey,
	trackTxReadOffset,
} from './_tx_read_offsets.js'

function session(id = 1n, partitionId = 0n, path = '/test/topic') {
	return new TopicPartitionSession(id, partitionId, path)
}

function tracker(partitionId = 0n) {
	let map: TopicTxReadOffsets = new Map()
	let current = session(1n, partitionId)

	return {
		map,
		current,
		track(offset: bigint, s = current) {
			trackTxReadOffset(map, s, offset)
		},
		ranges(path = '/test/topic') {
			let record = map.get(topicPartitionOffsetKey(path, partitionId))
			assert.ok(record)
			return record.ranges.map((range) => [range.start, range.end])
		},
	}
}

function staging(response: any) {
	let requests: any[] = []

	return {
		requests,
		driver: {
			createClient: () => ({
				async updateOffsetsInTransaction(request: any) {
					requests.push(request)
					return response
				},
			}),
		} as any,
	}
}

function operation(ready: boolean, status = StatusIds_StatusCode.SUCCESS) {
	return create(UpdateOffsetsInTransactionResponseSchema, {
		operation: create(OperationSchema, { ready, status }),
	})
}

function tx() {
	return { sessionId: 'session-id', transactionId: 'transaction-id' } as any
}

test.each([
	['consecutive', [5n, 6n, 7n], [[5n, 8n]]],
	['duplicate', [5n, 6n, 5n, 6n], [[5n, 7n]]],
	[
		'sparse gap',
		[10n, 20n, 15n],
		[
			[10n, 11n],
			[15n, 16n],
			[20n, 21n],
		],
	],
	[
		'extends end',
		[10n, 20n, 11n],
		[
			[10n, 12n],
			[20n, 21n],
		],
	],
	[
		'extends start',
		[10n, 20n, 9n],
		[
			[9n, 11n],
			[20n, 21n],
		],
	],
	['bridges ranges', [10n, 12n, 11n], [[10n, 13n]]],
	[
		'forward seek',
		[5n, 6n, 20n, 21n],
		[
			[5n, 7n],
			[20n, 22n],
		],
	],
] as const)('tracks %s offsets', (_name, offsets, expected) => {
	let t = tracker()
	for (let offset of offsets) t.track(offset)
	assert.deepStrictEqual(t.ranges(), expected)
})

test('fails closed when delivered offsets are not contiguous', () => {
	let t = tracker()
	t.track(10n)
	t.track(20n)

	assert.throws(
		() => collectTxReadOffsetUpdates(t.map),
		/not contiguous.*YDB cannot commit disjoint offset ranges/
	)

	t.track(11n)
	assert.throws(() => collectTxReadOffsetUpdates(t.map), /not contiguous/)
})

test('merges ranges when the gap is filled', () => {
	let t = tracker()
	t.track(10n)
	t.track(20n)
	for (let offset = 11n; offset < 20n; offset++) t.track(offset)

	assert.deepStrictEqual(collectTxReadOffsetUpdates(t.map), [
		{
			topicPath: '/test/topic',
			partitionId: 0n,
			range: { start: 10n, end: 21n },
		},
	])
})

test('keeps ranges across reconnects', () => {
	let t = tracker()
	let next = session(2n, 0n)

	assert.deepStrictEqual([t.current.partitionSessionId, next.partitionSessionId], [1n, 2n])

	t.track(10n)
	t.track(11n)
	t.current.stop()
	t.track(10n, next)
	t.track(11n, next)
	t.track(12n, next)

	assert.strictEqual(t.map.size, 1)
	assert.deepStrictEqual(t.ranges(), [[10n, 13n]])
})

test('bounds sparse tracking and fails with a concise error', () => {
	let t = tracker()
	for (let offset = 0n; offset < 400n; offset += 2n) t.track(offset)

	let record = t.map.get(topicPartitionOffsetKey('/test/topic', 0n))
	assert.ok(record)
	assert.strictEqual(record.truncated, true)
	assert.strictEqual(record.ranges.length, 0)
	assert.throws(
		() => collectTxReadOffsetUpdates(t.map),
		(error: Error) => {
			assert.match(error.message, /partition 0: more than 64 non-contiguous ranges/)
			assert.ok(error.message.length < 400)
			return true
		}
	)
})

test('propagates staging failures', async () => {
	let t = tracker()
	t.track(0n)
	let { driver } = staging(operation(true, StatusIds_StatusCode.ABORTED))

	await assert.rejects(
		_update_offsets_in_transaction(tx(), driver, 'consumer', collectTxReadOffsetUpdates(t.map)),
		/UpdateOffsetsInTransaction failed/
	)
})

test('accepts a ready SUCCESS operation and rejects a not-ready one', async () => {
	let t = tracker()
	t.track(0n)

	await assert.doesNotReject(
		_update_offsets_in_transaction(
			tx(),
			staging(operation(true)).driver,
			'consumer',
			collectTxReadOffsetUpdates(t.map)
		)
	)

	await assert.rejects(
		_update_offsets_in_transaction(
			tx(),
			staging(operation(false)).driver,
			'consumer',
			collectTxReadOffsetUpdates(t.map)
		),
		/operation is not ready/
	)
})

test('stages with the captured transaction identity', async () => {
	let t = tracker()
	t.track(0n)
	let { driver, requests } = staging(operation(true))

	await _update_offsets_in_transaction(
		tx(),
		driver,
		'consumer',
		collectTxReadOffsetUpdates(t.map),
		{ sessionId: 'captured-session', transactionId: 'captured-tx' }
	)

	assert.strictEqual(requests[0].tx.session, 'captured-session')
	assert.strictEqual(requests[0].tx.id, 'captured-tx')
})

test('builds unambiguous keys for delimiter-heavy topic paths', () => {
	assert.notStrictEqual(topicPartitionOffsetKey('/a|b', 1n), topicPartitionOffsetKey('/a', 1n))
	assert.notStrictEqual(topicPartitionOffsetKey('/a\0b', 1n), topicPartitionOffsetKey('/a', 1n))
	assert.notStrictEqual(topicPartitionOffsetKey('/a', 1n), topicPartitionOffsetKey('/a', 11n))
})

test('stages offsets for stopped and ended sessions', () => {
	let map: TopicTxReadOffsets = new Map()
	let stopped = session(1n, 0n, '/test/a')
	let ended = session(2n, 1n, '/test/b')

	trackTxReadOffset(map, stopped, 1n)
	stopped.stop()
	trackTxReadOffset(map, ended, 4n)
	ended.end()

	assert.deepStrictEqual(collectTxReadOffsetUpdates(map), [
		{ topicPath: '/test/a', partitionId: 0n, range: { start: 1n, end: 2n } },
		{ topicPath: '/test/b', partitionId: 1n, range: { start: 4n, end: 5n } },
	])
})

test('stages one contiguous range per partition, grouped by topic', async () => {
	let map: TopicTxReadOffsets = new Map()
	let a0 = session(1n, 0n, '/test/a')
	let a1 = session(2n, 1n, '/test/a')
	let b0 = session(3n, 0n, '/test/b')

	for (let offset of [0n, 1n, 2n]) trackTxReadOffset(map, a0, offset)
	trackTxReadOffset(map, a1, 9n)
	trackTxReadOffset(map, b0, 11n)

	let { driver, requests } = staging(operation(true))
	await _update_offsets_in_transaction(tx(), driver, 'consumer', collectTxReadOffsetUpdates(map))

	let request = requests[0]
	assert.deepStrictEqual(
		request.topics.map((topic: any) => topic.path),
		['/test/a', '/test/b']
	)
	assert.deepStrictEqual(
		request.topics[0].partitions.map((p: any) => [
			p.partitionId,
			p.partitionOffsets.map((o: any) => [o.start, o.end]),
		]),
		[
			[0n, [[0n, 3n]]],
			[1n, [[9n, 10n]]],
		]
	)
	assert.deepStrictEqual(
		request.topics[1].partitions[0].partitionOffsets.map((o: any) => [o.start, o.end]),
		[[11n, 12n]]
	)
})

test('forwards the transaction abort signal to staging', async () => {
	let t = tracker()
	t.track(0n)

	let controller = new AbortController()
	let driver = {
		createClient: () => ({
			updateOffsetsInTransaction: (_request: any, options: any) =>
				new Promise((_, reject) => {
					options.signal.addEventListener('abort', () => reject(options.signal.reason))
				}),
		}),
	} as any

	let stagingPromise = _update_offsets_in_transaction(
		tx(),
		driver,
		'consumer',
		collectTxReadOffsetUpdates(t.map),
		undefined,
		controller.signal
	)

	controller.abort(new Error('staging aborted'))
	await assert.rejects(stagingPromise, /staging aborted/)
})
