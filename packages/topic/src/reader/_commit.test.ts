import * as assert from 'node:assert'
import { create } from '@bufbuild/protobuf'
import { Codec, StreamReadMessage_CommitOffsetResponseSchema } from '@ydbjs/api/topic'
import { test } from 'vitest'

import { TopicPartitionSession } from '../partition-session.js'
import { TopicMessage } from '../message.js'
import { AsyncPriorityQueue } from '../queue.js'
import { _commit } from './_commit.js'
import { _on_commit_offset_response } from './_commit_offset_response.js'
import { _on_stop_partition_session_request } from './_stop_partition_session_request.js'
import type { TopicReaderState } from './types.js'

/**
 * Creates a minimal TopicReaderState for testing commit logic.
 */
function createMockState(partitionSessions: Map<bigint, TopicPartitionSession>): TopicReaderState {
	return {
		disposed: false,
		pendingCommits: new Map(),
		partitionSessions,
		outgoingQueue: new AsyncPriorityQueue(),
		// Unused in _commit, but required by type
		options: { topic: 'test', consumer: 'test' },
		driver: {} as any,
		topicsReadSettings: [],
		controller: new AbortController(),
		buffer: [],
		codecs: new Map(),
		maxBufferSize: 0n,
		freeBufferSize: 0n,
	}
}

/**
 * Creates a TopicMessage for testing.
 */
function createMessage(partitionSession: TopicPartitionSession, offset: bigint): TopicMessage {
	return new TopicMessage({
		partitionSession,
		producer: 'test-producer',
		payload: new Uint8Array(),
		codec: Codec.RAW,
		seqNo: 1n,
		offset,
	})
}

/**
 * Simulates a server acknowledgement for a single partition.
 */
function ack(state: TopicReaderState, partitionSessionId: bigint, committedOffset: bigint): void {
	_on_commit_offset_response(
		{
			pendingCommits: state.pendingCommits,
			partitionSessions: state.partitionSessions,
		},
		create(StreamReadMessage_CommitOffsetResponseSchema, {
			partitionsCommittedOffsets: [{ partitionSessionId, committedOffset }],
		})
	)
}

// https://github.com/ydb-platform/ydb-js-sdk/issues/552
// When messages are deleted by retention policy, there's a gap between
// committedOffset (e.g., 0) and the first available message offset (e.g., 29).
// The SDK must fill this gap by using nextCommitStartOffset as the range start.
test('fills gap between committedOffset and first message offset', async () => {
	let partitionSession = new TopicPartitionSession(1n, 0n, '/test/topic')

	// Simulate: server returned committedOffset=0, but first message has offset=29
	// (messages 0-28 were deleted by retention policy)
	partitionSession.nextCommitStartOffset = 0n

	let partitionSessions = new Map([[1n, partitionSession]])
	let state = createMockState(partitionSessions)

	let message = createMessage(partitionSession, 29n)

	// Start commit (don't await - we just check the outgoing request)
	_commit(state, message)

	// Check that outgoing queue has the commit request with gap filled
	let request = await state.outgoingQueue[Symbol.asyncIterator]().next()
	let commitRequest = request.value?.clientMessage

	assert.strictEqual(commitRequest?.case, 'commitOffsetRequest')
	if (commitRequest?.case === 'commitOffsetRequest') {
		let offsets = commitRequest.value.commitOffsets[0]?.offsets[0]
		// Gap should be filled: start=0 (not 29), end=30
		assert.strictEqual(offsets?.start, 0n)
		assert.strictEqual(offsets?.end, 30n)
	}

	// nextCommitStartOffset should be updated for subsequent commits
	assert.strictEqual(partitionSession.nextCommitStartOffset, 30n)
})

// https://github.com/ydb-platform/ydb-js-sdk/issues/552
// After committing, nextCommitStartOffset must be updated so subsequent
// commits don't re-fill the same gap.
test('updates nextCommitStartOffset after commit', async () => {
	let partitionSession = new TopicPartitionSession(1n, 0n, '/test/topic')
	partitionSession.nextCommitStartOffset = 50n

	let partitionSessions = new Map([[1n, partitionSession]])
	let state = createMockState(partitionSessions)

	let message = createMessage(partitionSession, 50n)
	_commit(state, message)

	assert.strictEqual(partitionSession.nextCommitStartOffset, 51n)

	// Second commit should use updated nextCommitStartOffset
	let message2 = createMessage(partitionSession, 51n)
	_commit(state, message2)

	assert.strictEqual(partitionSession.nextCommitStartOffset, 52n)
})

// A multi-partition commit must not resolve until the server acknowledges
// every partition, otherwise callers may treat a partially committed batch
// as fully committed.
test('resolves commit only after every partition is acknowledged', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let second = new TopicPartitionSession(2n, 1n, '/test/topic')
	let state = createMockState(
		new Map([
			[1n, first],
			[2n, second],
		])
	)

	let commitPromise = _commit(state, [createMessage(first, 0n), createMessage(second, 0n)])

	let observedStatus: 'pending' | 'fulfilled' | 'rejected' = 'pending'
	let observer = commitPromise.then(
		() => {
			observedStatus = 'fulfilled'
			return 'fulfilled' as const
		},
		() => {
			observedStatus = 'rejected'
			return 'rejected' as const
		}
	)

	ack(state, 1n, 1n)
	// Drain the microtask queue: the first acknowledgement alone must leave the
	// aggregate pending.
	await Promise.resolve()
	await Promise.resolve()
	assert.strictEqual(observedStatus, 'pending')
	assert.strictEqual(state.pendingCommits.has(2n), true)
	assert.strictEqual(state.pendingCommits.has(1n), false)

	ack(state, 2n, 1n)
	assert.strictEqual(await observer, 'fulfilled')
	assert.strictEqual(observedStatus, 'fulfilled')
	assert.strictEqual(state.pendingCommits.size, 0)
})

// A failure on any partition while others are still pending must reject the
// aggregate instead of waiting forever.
test('rejects commit when one partition fails', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let second = new TopicPartitionSession(2n, 1n, '/test/topic')
	let state = createMockState(
		new Map([
			[1n, first],
			[2n, second],
		])
	)

	let commitPromise = _commit(state, [createMessage(first, 0n), createMessage(second, 0n)])

	let failure = new Error('partition session stopped')
	for (let commit of state.pendingCommits.get(2n) ?? []) {
		commit.reject(failure)
	}

	await assert.rejects(commitPromise, /partition session stopped/)

	// The remaining partition is still allowed to settle without unhandled rejections.
	ack(state, 1n, 1n)
})

// A partition can fail after another one was already acknowledged. The
// aggregate must still reject rather than resolve with a partial commit.
test('rejects commit when a partition fails after an acknowledgement', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let second = new TopicPartitionSession(2n, 1n, '/test/topic')
	let state = createMockState(
		new Map([
			[1n, first],
			[2n, second],
		])
	)

	let commitPromise = _commit(state, [createMessage(first, 0n), createMessage(second, 0n)])

	ack(state, 1n, 1n)
	await Promise.resolve()
	await Promise.resolve()

	let failure = new Error('second partition failed')
	for (let commit of state.pendingCommits.get(2n) ?? []) {
		commit.reject(failure)
	}

	await assert.rejects(commitPromise, /second partition failed/)
})

function stopCtx(state: TopicReaderState, commitTimeoutMs = 1000): any {
	return {
		partitionSessions: state.partitionSessions,
		outgoingQueue: state.outgoingQueue,
		buffer: state.buffer,
		disposed: false,
		pendingCommits: state.pendingCommits,
		commitTimeoutMs,
	}
}

function gracefulStop(partitionSessionId: bigint): any {
	return { partitionSessionId, committedOffset: 0n, graceful: true }
}

// Graceful stop must wait on the actual per-partition promises. Previously it
// passed commit records to Promise.all, resolved immediately and rejected an
// ACK that was about to arrive.
test('graceful stop waits for a partition acknowledgement', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let second = new TopicPartitionSession(2n, 1n, '/test/topic')
	let state = createMockState(
		new Map([
			[1n, first],
			[2n, second],
		])
	)

	let commitPromise = _commit(state, [createMessage(first, 0n), createMessage(second, 0n)])
	ack(state, 1n, 1n)

	let stopPromise = _on_stop_partition_session_request(stopCtx(state), gracefulStop(2n))

	// Let a broken implementation resume and reject prematurely.
	await new Promise((resolve) => setTimeout(resolve, 0))

	ack(state, 2n, 1n)
	await stopPromise
	await assert.doesNotReject(commitPromise)
})

test('graceful stop rejects after the deadline without an acknowledgement', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let second = new TopicPartitionSession(2n, 1n, '/test/topic')
	let state = createMockState(
		new Map([
			[1n, first],
			[2n, second],
		])
	)

	let commitPromise = _commit(state, [createMessage(first, 0n), createMessage(second, 0n)])
	ack(state, 1n, 1n)

	await _on_stop_partition_session_request(stopCtx(state, 20), gracefulStop(2n))

	let error: unknown
	try {
		await commitPromise
	} catch (reason) {
		error = reason
	}
	assert.match(String(error), /Partition session stopped after timeout/)
})

// A reconnect/destroy may reject a pending commit while the detached stop
// handler is draining. The handler must settle without surfacing an unhandled
// rejection; the caller still observes the commit error.
test('graceful stop drains a commit rejected by disconnect', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let state = createMockState(new Map([[1n, first]]))
	let commitPromise = _commit(state, createMessage(first, 0n))

	let stopPromise = _on_stop_partition_session_request(stopCtx(state), gracefulStop(1n))

	for (let commit of state.pendingCommits.get(1n) ?? []) {
		commit.reject('disconnected')
	}

	await stopPromise
	await assert.rejects(commitPromise, /disconnected/)
})

// A commit accepted after the drain snapshot must be awaited too, as long as
// its ACK arrives before the shared deadline.
test('graceful stop drains a commit accepted while waiting', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let state = createMockState(new Map([[1n, first]]))
	let firstCommit = _commit(state, createMessage(first, 0n))

	let stopPromise = _on_stop_partition_session_request(stopCtx(state, 500), gracefulStop(1n))
	await Promise.resolve()

	let secondCommit = _commit(state, createMessage(first, 1n))
	ack(state, 1n, 1n) // resolves the snapshot, second commit is still pending
	await new Promise((resolve) => setTimeout(resolve, 0))
	ack(state, 1n, 2n) // arrives during the extended drain

	await stopPromise
	await assert.doesNotReject(firstCommit)
	await assert.doesNotReject(secondCommit)
})

test('graceful stop rejects commits still pending after the deadline', async () => {
	let first = new TopicPartitionSession(1n, 0n, '/test/topic')
	let state = createMockState(new Map([[1n, first]]))
	let firstCommit = _commit(state, createMessage(first, 0n))

	let stopPromise = _on_stop_partition_session_request(stopCtx(state, 20), gracefulStop(1n))
	await Promise.resolve()
	let secondCommit = _commit(state, createMessage(first, 1n))

	await stopPromise
	await assert.rejects(firstCommit, /timeout during graceful stop/)
	await assert.rejects(secondCommit, /timeout during graceful stop/)
})
