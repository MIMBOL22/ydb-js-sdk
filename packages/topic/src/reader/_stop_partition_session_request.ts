import * as assert from 'node:assert'
import { once } from 'node:events'
import { loggers } from '@ydbjs/debug'
import { _send_stop_partition_session_response } from './_stop_partition_session_response.js'
import type {
	StreamReadMessage_ReadResponse,
	StreamReadMessage_StopPartitionSessionRequest,
} from '@ydbjs/api/topic'
import type { AsyncPriorityQueue } from '../queue.js'
import type { StreamReadMessage_FromClient } from '@ydbjs/api/topic'
import type { TopicPartitionSession } from '../partition-session.js'
import type { TopicCommitPromise, onPartitionSessionStopCallback } from './types.js'

let dbg = loggers.topic.extend('reader')

export let _on_stop_partition_session_request = async function on_stop_partition_session_request(
	ctx: {
		readonly partitionSessions: Map<bigint, TopicPartitionSession>
		readonly outgoingQueue: AsyncPriorityQueue<StreamReadMessage_FromClient>
		readonly buffer: StreamReadMessage_ReadResponse[]
		readonly disposed: boolean
		readonly onPartitionSessionStop?: onPartitionSessionStopCallback
		readonly pendingCommits?: Map<bigint, TopicCommitPromise[]> // Optional for regular reader
		readonly commitTimeoutMs?: number
	},
	input: StreamReadMessage_StopPartitionSessionRequest
): Promise<void> {
	assert.ok(input.partitionSessionId, 'stopPartitionSessionRequest must have partitionSessionId')

	let partitionSession = ctx.partitionSessions.get(input.partitionSessionId)
	if (!partitionSession) {
		dbg.log(
			'error: stopPartitionSessionRequest for unknown partitionSessionId=%s',
			input.partitionSessionId
		)
		return
	}

	if (ctx.onPartitionSessionStop) {
		let committedOffset = input.committedOffset || 0n

		await ctx.onPartitionSessionStop(partitionSession, committedOffset).catch((err) => {
			dbg.log('error: onPartitionSessionStop error: %O', err)
			// Error will be propagated via exception if needed
		})
	}

	// If graceful stop is not requested, we can stop the partition session immediately.
	if (!input.graceful) {
		dbg.log(
			'stop partition session %s without graceful stop',
			partitionSession.partitionSessionId
		)
		partitionSession.stop()

		// Remove all messages from the buffer that belong to this partition session.
		for (let part of ctx.buffer) {
			let i = 0
			while (i < part.partitionData.length) {
				if (
					part.partitionData[i]!.partitionSessionId ===
					partitionSession.partitionSessionId
				) {
					part.partitionData.splice(i, 1)
				} else {
					i++
				}
			}
		}

		// Handle pending commits if they exist (only for regular reader)
		if (ctx.pendingCommits) {
			let pendingCommits = ctx.pendingCommits.get(partitionSession.partitionSessionId)
			if (pendingCommits) {
				// If there are pending commits for this partition session, reject them.
				for (let commit of pendingCommits) {
					commit.reject('Partition session stopped without graceful stop')
				}

				ctx.pendingCommits.delete(partitionSession.partitionSessionId)
			}
		}

		ctx.partitionSessions.delete(partitionSession.partitionSessionId)
		partitionSession = undefined

		return
	}

	// Handle graceful stop with pending commits (only for regular reader).
	// Keep draining commits accepted while we wait, until one shared deadline.
	if (ctx.pendingCommits && ctx.pendingCommits.has(partitionSession.partitionSessionId)) {
		let deadline = Date.now() + (ctx.commitTimeoutMs ?? 30_000)

		while (true) {
			let pending = ctx.pendingCommits.get(partitionSession.partitionSessionId)
			if (!pending || pending.length === 0) {
				break
			}

			let snapshot = pending.slice()
			let remaining = deadline - Date.now()
			if (remaining <= 0) {
				break
			}

			// allSettled: a commit rejected by reconnect/destroy must not turn
			// this detached stop handler into an unhandled rejection.
			// oxlint-disable-next-line no-await-in-loop
			let outcome = await Promise.race([
				Promise.allSettled(snapshot.map((commit) => commit.promise)).then(
					() => 'settled' as const
				),
				once(AbortSignal.timeout(remaining), 'abort').then(() => 'timeout' as const),
			])

			if (outcome === 'timeout') {
				break
			}

			let after = ctx.pendingCommits.get(partitionSession.partitionSessionId)
			if (!after || after.length === 0) {
				break
			}

			// No new commits were accepted while draining the snapshot.
			if (
				after.length === snapshot.length &&
				after.every((commit, index) => commit === snapshot[index])
			) {
				break
			}
		}
	}

	// The stream may have been disposed or this partition replaced during the
	// drain (reconnect/destroy); do not send a response into a dead stream.
	if (ctx.disposed) {
		return
	}
	if (ctx.partitionSessions.get(partitionSession.partitionSessionId) !== partitionSession) {
		return
	}
	if (partitionSession.isStopped || partitionSession.isEnded) {
		return
	}

	if (ctx.pendingCommits && ctx.pendingCommits.has(partitionSession.partitionSessionId)) {
		// If there are pending commits for this partition session, reject them.
		for (let commit of ctx.pendingCommits.get(partitionSession.partitionSessionId)!) {
			commit.reject('Partition session stopped after timeout during graceful stop')
		}

		ctx.pendingCommits.delete(partitionSession.partitionSessionId)
	}

	try {
		_send_stop_partition_session_response({
			queue: ctx.outgoingQueue,
			partitionSessionId: partitionSession.partitionSessionId,
		})
	} catch (error) {
		// Queue may be closed by a concurrent reconnect/destroy.
		dbg.log('stop partition session response skipped: %O', error)
	}
}
