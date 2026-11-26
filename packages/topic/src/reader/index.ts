import type { StreamReadMessage_FromClient } from '@ydbjs/api/topic'
import type { Driver } from '@ydbjs/core'
import { loggers } from '@ydbjs/debug'

import { AsyncPriorityQueue } from '../queue.js'
import { defaultCodecMap } from '../codec.js'

import type {
	TopicReader,
	TopicReaderOptions,
	TopicReaderState,
	TopicTxReader,
	TopicTxReaderState,
} from './types.js'
import { _send_update_token_request } from './_update_token.js'
import { _parse_topics_read_settings } from './_topics_config.js'
import { _consume_stream } from './_consume_stream.js'
import { _consume_stream_tx } from './_consume_stream_tx.js'
import { _read } from './_read.js'
import { _commit } from './_commit.js'
import { _update_offsets_in_transaction } from './_update_offsets_in_transaction.js'
import { collectTxReadOffsetUpdates } from './_tx_read_offsets.js'
import {
	type TopicTxIdentity,
	releaseTxReaderConsumer,
	reserveTxReaderConsumer,
} from './_tx_reader_registry.js'
import {
	_create_disposal_functions,
	_initialize_codecs,
	_start_background_token_refresher,
} from './_shared.js'
import type { TX } from '../tx.js'

let dbg = loggers.topic.extend('reader')

// Timeout for graceful shutdown waiting for pending commits
let GRACEFUL_SHUTDOWN_TIMEOUT_MS = 30_000

export const createTopicReader = function createTopicReader(
	driver: Driver,
	options: TopicReaderOptions
): TopicReader {
	options.updateTokenIntervalMs ??= 60_000 // Default is 60 seconds.

	let state: TopicReaderState = {
		driver,
		options,
		topicsReadSettings: _parse_topics_read_settings(options.topic),

		// Control
		controller: new AbortController(),
		disposed: false,

		// Data structures
		outgoingQueue: new AsyncPriorityQueue<StreamReadMessage_FromClient>(),
		buffer: [],
		partitionSessions: new Map(),
		pendingCommits: new Map(),
		codecs: new Map(defaultCodecMap),

		// Buffer management
		maxBufferSize: options.maxBufferBytes ?? 4n * 1024n * 1024n, // Reduced to 4MB for faster parsing
		freeBufferSize: options.maxBufferBytes ?? 4n * 1024n * 1024n, // Reduced to 4MB for faster parsing
	}
	// Initialize custom codecs if provided
	_initialize_codecs(state.codecs, options.codecMap)

	// Start consuming the stream immediately.
	;(async function stream() {
		try {
			await _consume_stream(state)
		} catch (error) {
			if (!state.controller.signal.aborted) {
				dbg.log('error occurred while streaming: %O', error)
				// Stream died unexpectedly (retry budget exhausted or non-retryable
				// error). Destroy the reader so that any pending read() calls are
				// unblocked rather than polling forever.
				destroy(error)
			}
		} finally {
			dbg.log('stream closed')
		}
	})()

	// Update the token periodically to ensure that the reader has a valid token.
	// This is useful to avoid token expiration and to ensure that the reader can continue to read messages from the topic.
	// The update token interval is configurable and defaults to 60 seconds.
	_start_background_token_refresher(
		state.driver,
		state.outgoingQueue,
		options.updateTokenIntervalMs,
		state.controller.signal
	)

	async function close() {
		if (state.disposed) return

		// Stop accepting new messages and requests
		state.outgoingQueue.close()

		// Wait for all pending commits to resolve with a timeout
		let pendingCommitPromises: Promise<void>[] = []
		for (let commits of state.pendingCommits.values()) {
			for (let commit of commits) {
				pendingCommitPromises.push(
					new Promise<void>((resolve) => {
						let originalResolve = commit.resolve
						let originalReject = commit.reject

						commit.resolve = () => {
							originalResolve()
							resolve()
						}

						commit.reject = (reason) => {
							originalReject(reason)
							resolve() // Still resolve our promise even if commit was rejected
						}
					})
				)
			}
		}

		if (pendingCommitPromises.length > 0) {
			try {
				// Wait for all pending commits or timeout after 30 seconds
				await Promise.race([
					Promise.all(pendingCommitPromises),
					new Promise<void>((resolve) =>
						setTimeout(resolve, GRACEFUL_SHUTDOWN_TIMEOUT_MS)
					),
				])
			} catch (err) {
				dbg.log('error during close: %O', err)
				throw err
			}
		}

		dbg.log('reader closed gracefully')

		// Now safely dispose - this will stop the token refresher via AbortSignal
		destroy(new Error('TopicReader closed'))
	}

	function destroy(reason: unknown) {
		if (state.disposed) return

		// Immediate shutdown - reject all pending commits
		for (let commits of state.pendingCommits.values()) {
			for (let commit of commits) {
				commit.reject(reason || new Error('TopicReader destroyed'))
			}
		}

		state.disposed = true
		state.outgoingQueue.dispose()
		state.pendingCommits.clear()
		state.partitionSessions.clear()
		state.buffer.length = 0
		state.controller.abort(reason)
	}

	return {
		read(options = {}) {
			return _read(
				{
					disposed: state.disposed,
					controller: state.controller,
					buffer: state.buffer,
					partitionSessions: state.partitionSessions,
					codecs: state.codecs,
					outgoingQueue: state.outgoingQueue,
					maxBufferSize: state.maxBufferSize,
					freeBufferSize: state.freeBufferSize,
					updateFreeBufferSize: (releasedBytes: bigint) => {
						state.freeBufferSize += releasedBytes
					},
				},
				options
			)
		},

		commit(input) {
			return _commit(state, input)
		},

		close,
		destroy,

		[Symbol.dispose]() {
			destroy(new Error('TopicReader disposed'))
		},

		async [Symbol.asyncDispose]() {
			// Graceful async disposal: close and wait for stream to finish
			try {
				await close()
			} catch (error) {
				dbg.log('error during async dispose close: %O', error)
			}
		},
	}
}

export const createTopicTxReader = function createTopicTxReader(
	tx: TX,
	driver: Driver,
	options: TopicReaderOptions
): TopicTxReader {
	// Capture the identity before any mutable TX field can be overwritten by
	// another transaction on the same query client.
	let txIdentity: TopicTxIdentity = {
		sessionId: tx.sessionId,
		transactionId: tx.transactionId,
	}

	// Snapshot the caller's options: later mutations of the original object must
	// not change what this reader stages or which consumer claim it releases.
	let consumer = options.consumer
	let readerOptions: TopicReaderOptions = { ...options, consumer }
	readerOptions.updateTokenIntervalMs ??= 60_000 // Default is 60 seconds.

	let state: TopicTxReaderState = {
		tx,
		driver,
		options: readerOptions,
		topicsReadSettings: _parse_topics_read_settings(readerOptions.topic),

		// Control
		controller: new AbortController(),
		disposed: false,

		// Data structures
		outgoingQueue: new AsyncPriorityQueue<StreamReadMessage_FromClient>(),
		buffer: [],
		partitionSessions: new Map(),
		codecs: new Map(defaultCodecMap),

		// Buffer management
		maxBufferSize: options.maxBufferBytes ?? 4n * 1024n * 1024n, // Reduced to 4MB for faster parsing
		freeBufferSize: options.maxBufferBytes ?? 4n * 1024n * 1024n, // Reduced to 4MB for faster parsing

		// Transaction support - track read offsets for commit hook
		readOffsets: new Map(),
	}

	// Initialize custom codecs if provided
	_initialize_codecs(state.codecs, options.codecMap)

	// Reserve only after all synchronous initialization that can throw, so a
	// failed construction does not leave a stale consumer claim behind.
	reserveTxReaderConsumer(txIdentity, consumer)

	try {
		// Register precommit hook to send updateOffsetsInTransaction
		tx.onCommit(async () => {
			// Stage every delivered range. Offset staging uses the stable
			// (topicPath, partitionId) identity, so it does not depend on the local
			// partition session still being alive after a reconnect.
			let updates = collectTxReadOffsetUpdates(state.readOffsets)

			dbg.log('Updating offsets in transaction for %d partitions', updates.length)

			if (updates.length > 0) {
				await _update_offsets_in_transaction(
					tx,
					state.driver,
					consumer,
					updates,
					txIdentity
				)
			}

			closeWithReason('Transaction committed')
		})

		tx.onRollback((error) => {
			dbg.log('transaction rollback detected, closing tx reader: %O', error)
			// The transaction is over and can no longer stage offsets; drop tracking
			// only here, never on reader/stream disposal.
			state.readOffsets.clear()
			releaseTxReaderConsumer(txIdentity, consumer)
			closeWithReason('Transaction rolled back', error)
		})

		tx.onClose(() => {
			// The transaction is closed (committed or rolled back), so the tracked
			// offsets are no longer needed.
			state.readOffsets.clear()
			releaseTxReaderConsumer(txIdentity, consumer)

			if (state.disposed) {
				return
			}

			dbg.log('transaction closed, disposing tx reader')
			closeWithReason('Transaction closed')
		})

		// Start consuming the stream immediately.
		void (async function stream() {
			try {
				await _consume_stream_tx(state)
			} catch (error) {
				if (!state.controller.signal.aborted) {
					dbg.log('error occurred while streaming: %O', error)
				}
			} finally {
				dbg.log('tx stream closed')
				destroy(new Error('Stream closed'))
			}
		})()

		// Update the token periodically to ensure that the reader has a valid token.
		_start_background_token_refresher(
			state.driver,
			state.outgoingQueue,
			readerOptions.updateTokenIntervalMs,
			state.controller.signal
		)
	} catch (error) {
		releaseTxReaderConsumer(txIdentity, consumer)
		throw error
	}

	function closeWithReason(reason: string, error?: unknown) {
		if (state.disposed) {
			return
		}

		dbg.log('tx reader closing (%s)', reason)
		destroy(error ?? new Error(reason))
	}

	async function close() {
		closeWithReason('TopicTxReader closed')
	}

	function destroy(reason: unknown) {
		if (state.disposed) return

		state.disposed = true
		state.outgoingQueue.dispose()
		// Tracked read offsets are intentionally retained: an explicit reader
		// close or a terminal stream shutdown must not prevent the transaction
		// pre-commit hook from staging already delivered offsets. The map is
		// cleared by the tx.onRollback / tx.onClose handlers.
		state.partitionSessions.clear()
		state.buffer.length = 0
		state.controller.abort(reason)
	}

	return {
		read(readOptions = {}) {
			return _read(
				{
					disposed: state.disposed,
					controller: state.controller,
					buffer: state.buffer,
					partitionSessions: state.partitionSessions,
					codecs: state.codecs,
					outgoingQueue: state.outgoingQueue,
					maxBufferSize: state.maxBufferSize,
					freeBufferSize: state.freeBufferSize,
					readOffsets: state.readOffsets,
					updateFreeBufferSize: (releasedBytes: bigint) => {
						state.freeBufferSize += releasedBytes
					},
				},
				readOptions
			)
		},

		close,
		destroy,
	}
}

// Re-export types for compatibility
export type { TopicReaderOptions, TopicReader, TopicTxReader } from './types.js'
