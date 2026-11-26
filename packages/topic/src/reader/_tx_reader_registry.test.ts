import * as assert from 'node:assert'
import { test } from 'vitest'

import { createTopicTxReader } from './index.js'
import { txReaderIdentityKey } from './_tx_reader_registry.js'
import type { TX } from '../tx.js'

function createTxDouble() {
	let commit: Array<() => void> = []
	let rollback: Array<(error: unknown) => void> = []
	let close: Array<(committed: boolean) => void> = []
	let tx = {
		sessionId: 'session-id',
		transactionId: 'transaction-id',
		signal: new AbortController().signal,
		onCommit(fn: () => void) {
			commit.push(fn)
		},
		onRollback(fn: (error: unknown) => void) {
			rollback.push(fn)
		},
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

let options = (consumer: string) => ({
	topic: '/test/topic',
	consumer,
	updateTokenIntervalMs: 60_000,
})

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

test('rejects a second reader for the same identity and consumer', async () => {
	let { tx, close } = createTxDouble()
	let reader = createTopicTxReader(tx, idleDriver(), options('dup-consumer'))

	try {
		assert.throws(
			() => createTopicTxReader(tx, idleDriver(), options('dup-consumer')),
			/only one transactional reader until the transaction ends/
		)
	} finally {
		reader.destroy(new Error('test cleanup'))
		await close()
	}
})

// Parsing/codecs run before reservation, so a failed construction must not
// leave a stale claim for the same consumer.
test('failed construction does not leave a stale consumer claim', async () => {
	let { tx, close } = createTxDouble()

	assert.throws(
		() =>
			createTopicTxReader(tx, idleDriver(), {
				...options('stale-consumer'),
				codecMap: {} as any,
			}),
		/not iterable/
	)

	let reader = createTopicTxReader(tx, idleDriver(), options('stale-consumer'))

	try {
		assert.ok(reader)
	} finally {
		reader.destroy(new Error('test cleanup'))
		await close()
	}
})
