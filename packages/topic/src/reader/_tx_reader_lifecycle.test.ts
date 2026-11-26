import * as assert from 'node:assert'
import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import { Codec } from '@ydbjs/api/topic'
import { test } from 'vitest'

import { createTopicTxReader } from './index.js'
import type { TopicMessage } from '../message.js'
import type { TX } from '../tx.js'
import type { TopicReaderOptions, TopicTxReader } from './types.js'

type CommitHook = (signal?: AbortSignal) => Promise<void> | void
type RollbackHook = (error: unknown, signal?: AbortSignal) => Promise<void> | void
type CloseHook = (committed: boolean, signal?: AbortSignal) => Promise<void> | void

let txCounter = 0

function createTxDouble() {
	let commit: CommitHook[] = []
	let rollback: RollbackHook[] = []
	let close: CloseHook[] = []
	let abort = new AbortController()
	txCounter += 1

	let tx = {
		sessionId: `session-${txCounter}`,
		transactionId: `transaction-${txCounter}`,
		signal: abort.signal,
		onCommit(fn: CommitHook) {
			commit.push(fn)
		},
		onRollback(fn: RollbackHook) {
			rollback.push(fn)
		},
		onClose(fn: CloseHook) {
			close.push(fn)
		},
	} as TX

	return {
		tx,
		async commit() {
			for (let hook of commit) {
				// oxlint-disable-next-line no-await-in-loop
				await hook()
			}
		},
		async rollback(error: unknown) {
			for (let hook of rollback) {
				// oxlint-disable-next-line no-await-in-loop
				await hook(error)
			}
		},
		async close(committed: boolean) {
			for (let hook of close) {
				// oxlint-disable-next-line no-await-in-loop
				await hook(committed)
			}
		},
	}
}

function createChannel() {
	let queue: any[] = []
	let wake: (() => void) | undefined
	let closed = false
	let iterable = (async function* () {
		while (true) {
			if (queue.length) {
				yield queue.shift()
				continue
			}
			if (closed) return
			// oxlint-disable-next-line no-await-in-loop
			await new Promise<void>((resolve) => {
				wake = resolve
			})
		}
	})()

	return {
		iterable,
		push(chunk: any) {
			queue.push(chunk)
			let resume = wake
			wake = undefined
			resume?.()
		},
		close() {
			closed = true
			let resume = wake
			wake = undefined
			resume?.()
		},
	}
}

function createHarness(stageStatus = StatusIds_StatusCode.SUCCESS, channels = [createChannel()]) {
	let staged: any[] = []
	let startedSessions: bigint[] = []
	let reads = 0
	let txDouble = createTxDouble()
	let options: TopicReaderOptions = {
		topic: '/test/topic',
		consumer: 'consumer',
		updateTokenIntervalMs: 60_000,
		onPartitionSessionStart: async (partitionSession) => {
			startedSessions.push(partitionSession.partitionSessionId)
		},
	}
	let driver = {
		ready: async () => {},
		token: Promise.resolve('token'),
		createClient: () => ({
			streamRead: () => (channels[reads++] ?? channels[channels.length - 1]!).iterable,
			async updateOffsetsInTransaction(request: any) {
				staged.push(request)
				return { operation: { ready: true, status: stageStatus } }
			},
		}),
	} as any

	return {
		reader: createTopicTxReader(txDouble.tx, driver, options),
		tx: txDouble.tx,
		driver,
		options,
		channels,
		startedSessions,
		staged,
		reads: () => reads,
		commit: txDouble.commit,
		rollback: txDouble.rollback,
		closeTransaction: txDouble.close,
	}
}

function startPartition(
	channel: ReturnType<typeof createChannel>,
	id: bigint,
	partitionId: bigint
) {
	channel.push({
		status: StatusIds_StatusCode.SUCCESS,
		serverMessage: {
			case: 'startPartitionSessionRequest',
			value: {
				partitionSession: {
					partitionSessionId: id,
					partitionId,
					path: '/test/topic',
				},
				partitionOffsets: { start: 0n, end: 1000n },
				committedOffset: 0n,
			},
		},
	})
}

function deliver(channel: ReturnType<typeof createChannel>, id: bigint, offsets: bigint[]) {
	channel.push({
		status: StatusIds_StatusCode.SUCCESS,
		serverMessage: {
			case: 'readResponse',
			value: {
				bytesSize: 1n,
				partitionData: [
					{
						partitionSessionId: id,
						batches: [
							{
								producerId: 'producer',
								codec: Codec.UNSPECIFIED,
								messageData: offsets.map((offset) => ({
									offset,
									seqNo: offset,
									data: new Uint8Array(),
									uncompressedSize: 0n,
								})),
							},
						],
					},
				],
			},
		},
	})
}

async function readMessages(reader: TopicTxReader, count: number): Promise<TopicMessage[]> {
	let received: TopicMessage[] = []
	for await (let batch of reader.read({
		limit: count,
		waitMs: 500,
		signal: AbortSignal.timeout(5000),
	})) {
		received.push(...batch)
	}
	return received
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000) {
	let start = Date.now()
	while (!predicate()) {
		assert.ok(Date.now() - start < timeoutMs, 'waitFor timed out')
		// oxlint-disable-next-line no-await-in-loop
		await new Promise((resolve) => setTimeout(resolve, 5))
	}
}

function ranges(request: any): bigint[][] {
	return request.topics[0].partitions[0].partitionOffsets.map((range: any) => [
		range.start,
		range.end,
	])
}

test('stages offsets when the reader is closed before commit', async () => {
	let h = createHarness()
	try {
		startPartition(h.channels[0]!, 1n, 0n)
		deliver(h.channels[0]!, 1n, [0n, 1n])
		assert.strictEqual((await readMessages(h.reader, 2)).length, 2)

		await h.reader.close()
		await h.commit()

		assert.strictEqual(h.staged.length, 1)
		assert.deepStrictEqual(ranges(h.staged[0]), [[0n, 2n]])
	} finally {
		h.channels.forEach((c) => c.close())
		h.reader.destroy(new Error('test cleanup'))
	}
})

test('stages offsets delivered before a reconnect with no new delivery', async () => {
	let first = createChannel()
	let second = createChannel()
	let h = createHarness(StatusIds_StatusCode.SUCCESS, [first, second])

	try {
		startPartition(first, 1n, 0n)
		deliver(first, 1n, [7n, 8n])
		assert.strictEqual((await readMessages(h.reader, 2)).length, 2)

		first.push({
			status: StatusIds_StatusCode.UNAVAILABLE,
			serverMessage: {},
			issues: [],
		})
		await waitFor(() => h.reads() === 2)
		startPartition(second, 2n, 0n)
		await waitFor(() => h.startedSessions.length === 2)
		assert.deepStrictEqual(h.startedSessions, [1n, 2n])

		await h.commit()
		assert.strictEqual(h.staged.length, 1)
		assert.strictEqual(h.staged[0].topics[0].path, '/test/topic')
		assert.strictEqual(h.staged[0].topics[0].partitions[0].partitionId, 0n)
		assert.deepStrictEqual(ranges(h.staged[0]), [[7n, 9n]])
	} finally {
		first.close()
		second.close()
		h.reader.destroy(new Error('test cleanup'))
	}
})

test('clears tracked offsets on rollback', async () => {
	let h = createHarness()
	try {
		startPartition(h.channels[0]!, 1n, 0n)
		deliver(h.channels[0]!, 1n, [0n])
		await readMessages(h.reader, 1)

		await h.rollback(new Error('rolled back'))
		await h.commit()
		assert.strictEqual(h.staged.length, 0)
	} finally {
		h.channels.forEach((c) => c.close())
		h.reader.destroy(new Error('test cleanup'))
	}
})

test('staging failure rejects the commit hook', async () => {
	let h = createHarness(StatusIds_StatusCode.ABORTED)
	try {
		startPartition(h.channels[0]!, 1n, 0n)
		deliver(h.channels[0]!, 1n, [0n])
		await readMessages(h.reader, 1)

		await assert.rejects(h.commit(), /UpdateOffsetsInTransaction failed/)
		assert.strictEqual(h.staged.length, 1)
		assert.doesNotThrow(() => h.reader.read())
	} finally {
		h.channels.forEach((c) => c.close())
		h.reader.destroy(new Error('test cleanup'))
	}
})

test('snapshots the consumer from the creation options', async () => {
	let h = createHarness()
	try {
		startPartition(h.channels[0]!, 1n, 0n)
		deliver(h.channels[0]!, 1n, [0n])
		await readMessages(h.reader, 1)

		h.options.consumer = 'mutated-consumer'

		await h.commit()
		assert.strictEqual(h.staged[0].consumer, 'consumer')

		// The claim was released under the original consumer.
		await h.closeTransaction(true)
		let retry = createTopicTxReader(h.tx, h.driver, {
			topic: '/test/topic',
			consumer: 'consumer',
			updateTokenIntervalMs: 60_000,
		})
		retry.destroy(new Error('test cleanup'))
	} finally {
		h.channels.forEach((c) => c.close())
		h.reader.destroy(new Error('test cleanup'))
	}
})

test('clears tracked offsets after the transaction closes', async () => {
	let h = createHarness()
	try {
		startPartition(h.channels[0]!, 1n, 0n)
		deliver(h.channels[0]!, 1n, [0n])
		await readMessages(h.reader, 1)

		await h.commit()
		assert.strictEqual(h.staged.length, 1)

		await h.closeTransaction(true)
		await h.commit()
		assert.strictEqual(h.staged.length, 1)
	} finally {
		h.channels.forEach((c) => c.close())
		h.reader.destroy(new Error('test cleanup'))
	}
})
