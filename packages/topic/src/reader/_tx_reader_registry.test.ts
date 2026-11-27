import * as assert from 'node:assert'
import { test } from 'vitest'

import { createTopicTxReader } from './index.js'
import { _parse_topics_read_settings } from './_topics_config.js'
import {
	type TopicTxIdentity,
	releaseTxReaderSubscriptions,
	reserveTxReaderSubscriptions,
	txReaderIdentityKey,
} from './_tx_reader_registry.js'
import type { TX } from '../tx.js'

function identity(value: string): TopicTxIdentity {
	return { sessionId: `session-${value}`, transactionId: `tx-${value}` }
}

function createTxDouble(id: TopicTxIdentity) {
	let close: Array<(committed: boolean) => void> = []
	let tx = {
		sessionId: id.sessionId,
		transactionId: id.transactionId,
		signal: new AbortController().signal,
		onCommit() {},
		onRollback() {},
		onClose(fn: (committed: boolean) => void) {
			close.push(fn)
		},
	} as TX

	return {
		tx,
		async close() {
			for (let hook of close) {
				// oxlint-disable-next-line no-await-in-loop
				await hook(true)
			}
		},
	}
}

function idleDriver() {
	return {
		ready: async () => {},
		token: Promise.resolve('token'),
		createClient: () => ({
			streamRead: () => ({
				[Symbol.asyncIterator]: () => ({
					next: () => new Promise(() => {}),
				}),
			}),
			updateOffsetsInTransaction: async () => ({
				operation: { ready: true, status: 400000 },
			}),
		}),
	} as any
}

test('keys readers by captured transaction identity', () => {
	assert.strictEqual(
		txReaderIdentityKey({ sessionId: 'session-1', transactionId: 'tx-1' }),
		JSON.stringify(['session-1', 'tx-1'])
	)
	assert.notStrictEqual(
		txReaderIdentityKey({ sessionId: 'session-1', transactionId: 'tx-1' }),
		txReaderIdentityKey({ sessionId: 'session-2', transactionId: 'tx-1' })
	)
})

test('allows disjoint topics for the same consumer', () => {
	let id = identity('disjoint-topics')
	let first = reserveTxReaderSubscriptions(id, 'consumer', '/test/a')
	let second = reserveTxReaderSubscriptions(id, 'consumer', '/test/b')

	assert.ok(first.token)
	assert.ok(second.token)
	releaseTxReaderSubscriptions(id, 'consumer', first)
	releaseTxReaderSubscriptions(id, 'consumer', second)
})

test('allows disjoint partition ids on the same topic', () => {
	let id = identity('disjoint-partitions')
	let first = reserveTxReaderSubscriptions(id, 'consumer', {
		path: '/test/a',
		partitionIds: [0n],
	})
	let second = reserveTxReaderSubscriptions(id, 'consumer', {
		path: '/test/a',
		partitionIds: [1n],
	})

	assert.ok(first.token)
	assert.ok(second.token)
	releaseTxReaderSubscriptions(id, 'consumer', first)
	releaseTxReaderSubscriptions(id, 'consumer', second)
})

test('rejects overlapping topic/partition subscriptions', () => {
	let id = identity('overlap')
	let all = reserveTxReaderSubscriptions(id, 'consumer', '/test/a')

	assert.throws(
		() =>
			reserveTxReaderSubscriptions(id, 'consumer', {
				path: '/test/a',
				partitionIds: [0n],
			}),
		/overlapping topic "\/test\/a"/
	)

	releaseTxReaderSubscriptions(id, 'consumer', all)

	let explicit = reserveTxReaderSubscriptions(id, 'consumer', {
		path: '/test/a',
		partitionIds: [0n, 1n],
	})

	assert.throws(
		() =>
			reserveTxReaderSubscriptions(id, 'consumer', {
				path: '/test/a',
				partitionIds: [1n, 2n],
			}),
		/overlapping topic/
	)
	assert.throws(
		() => reserveTxReaderSubscriptions(id, 'consumer', '/test/a'),
		/overlapping topic/
	)

	releaseTxReaderSubscriptions(id, 'consumer', explicit)
})

test('stale release does not remove a newer claim', () => {
	let id = identity('stale-release')
	let stale = reserveTxReaderSubscriptions(id, 'consumer', '/test/a')
	releaseTxReaderSubscriptions(id, 'consumer', stale)

	let current = reserveTxReaderSubscriptions(id, 'consumer', '/test/a')
	// A late rollback hook from the old reader must be a no-op for the new claim.
	releaseTxReaderSubscriptions(id, 'consumer', stale)

	assert.throws(
		() => reserveTxReaderSubscriptions(id, 'consumer', '/test/a'),
		/overlapping topic/
	)

	releaseTxReaderSubscriptions(id, 'consumer', current)
	reserveTxReaderSubscriptions(id, 'consumer', '/test/a')
})

// Parsing/codecs run before reservation, so a failed construction must not
// leave a stale claim for the same consumer.
test('failed construction does not leave a stale claim', async () => {
	let id = identity('stale-claim')
	let { tx, close } = createTxDouble(id)

	assert.throws(
		() =>
			createTopicTxReader(tx, idleDriver(), {
				topic: '/test/a',
				consumer: 'stale-consumer',
				codecMap: {} as any,
				updateTokenIntervalMs: 60_000,
			}),
		/not iterable/
	)

	let reader = createTopicTxReader(tx, idleDriver(), {
		topic: '/test/a',
		consumer: 'stale-consumer',
		updateTokenIntervalMs: 60_000,
	})

	try {
		assert.ok(reader)
	} finally {
		reader.destroy(new Error('test cleanup'))
		await close()
	}
})

// The stream settings and the reservation must use the same partition-id
// snapshot; mutating the caller's array afterwards cannot move the claim.
test('snapshots partition ids for the stream and the claim', async () => {
	let id = identity('partition-snapshot')
	let { tx, close } = createTxDouble(id)
	let partitionIds = [0n]

	let reader = createTopicTxReader(tx, idleDriver(), {
		topic: { path: '/test/a', partitionIds },
		consumer: 'consumer',
		updateTokenIntervalMs: 60_000,
	})

	try {
		partitionIds[0] = 1n

		assert.throws(
			() =>
				reserveTxReaderSubscriptions(id, 'consumer', {
					path: '/test/a',
					partitionIds: [0n],
				}),
			/overlapping topic/
		)

		let other = reserveTxReaderSubscriptions(id, 'consumer', {
			path: '/test/a',
			partitionIds: [1n],
		})
		releaseTxReaderSubscriptions(id, 'consumer', other)
	} finally {
		reader.destroy(new Error('test cleanup'))
		await close()
	}
})

test('snapshots partition ids in parsed stream settings', () => {
	let partitionIds = [0n]
	let settings = _parse_topics_read_settings({ path: '/test/a', partitionIds })

	partitionIds[0] = 1n

	assert.deepStrictEqual(settings[0]!.partitionIds, [0n])
})
