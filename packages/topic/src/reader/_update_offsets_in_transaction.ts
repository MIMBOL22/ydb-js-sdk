import { TopicServiceDefinition } from '@ydbjs/api/topic'
import {
	OffsetsRangeSchema,
	TransactionIdentitySchema,
	UpdateOffsetsInTransactionRequestSchema,
	UpdateOffsetsInTransactionRequest_TopicOffsetsSchema,
	UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsetsSchema,
} from '@ydbjs/api/topic'
import { StatusIds_StatusCode } from '@ydbjs/api/operation'
import { create } from '@bufbuild/protobuf'
import type { Driver } from '@ydbjs/core'
import { loggers } from '@ydbjs/debug'
import type { TX } from '../tx.js'
import type { TopicTxOffsetRange, TopicTxReadOffsetUpdate } from './_tx_read_offsets.js'
import type { TopicTxIdentity } from './_tx_reader_registry.js'

let dbg = loggers.topic.extend('reader')

export let _update_offsets_in_transaction = async function updateOffsetsInTransaction(
	tx: TX,
	driver: Driver,
	consumer: string,
	updates: TopicTxReadOffsetUpdate[],
	identity?: TopicTxIdentity
): Promise<void> {
	if (updates.length === 0) {
		dbg.log('no offsets to update in transaction')
		return
	}

	// Group updates by topic path and partition id. Partition identity is the
	// stable (topicPath, partitionId) pair; it does not depend on a live stream
	// partition session, so offsets can be staged after reconnects and stops.
	let topicMap = new Map<
		string,
		{
			path: string
			partitions: Map<bigint, TopicTxOffsetRange>
		}
	>()

	for (let update of updates) {
		let topicEntry = topicMap.get(update.topicPath)
		if (!topicEntry) {
			topicEntry = { path: update.topicPath, partitions: new Map() }
			topicMap.set(update.topicPath, topicEntry)
		}

		// One contiguous range per partition: YDB aborts the commit when the
		// request contains disjoint ranges. `collectTxReadOffsetUpdates` rejects
		// non-contiguous delivery before this function is called.
		topicEntry.partitions.set(update.partitionId, update.range)
	}

	if (topicMap.size === 0) {
		dbg.log('no offsets to update in transaction')
		return
	}

	// Build the request with a single `[start, end)` range per partition.
	// Prefer the identity captured when the reader was created: a shared query
	// client mutates `tx.sessionId`/`tx.transactionId` when another transaction
	// begins on the same callable.
	let sessionId = identity?.sessionId ?? tx.sessionId
	let transactionId = identity?.transactionId ?? tx.transactionId

	let topics = Array.from(topicMap.values())
	let request = create(UpdateOffsetsInTransactionRequestSchema, {
		tx: create(TransactionIdentitySchema, {
			id: transactionId,
			session: sessionId,
		}),
		topics: topics.map((topic) =>
			create(UpdateOffsetsInTransactionRequest_TopicOffsetsSchema, {
				path: topic.path,
				partitions: Array.from(topic.partitions.entries()).map(([partitionId, range]) =>
					create(UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsetsSchema, {
						partitionId,
						partitionOffsets: [
							create(OffsetsRangeSchema, {
								start: range.start,
								end: range.end, // exclusive end
							}),
						],
					})
				),
			})
		),
		consumer,
	})

	dbg.log(
		'sending updateOffsetsInTransaction request for %d topics, %d total partitions',
		topics.length,
		topics.reduce((sum, topic) => sum + topic.partitions.size, 0)
	)

	// Send the request. Any failure is propagated to the pre-commit hook so the
	// transaction cannot commit without its delivered offsets.
	let client = driver.createClient(TopicServiceDefinition)
	let response = await client.updateOffsetsInTransaction(request)

	if (response.operation?.ready === false) {
		throw new Error('UpdateOffsetsInTransaction operation is not ready')
	}

	if (response.operation?.status !== StatusIds_StatusCode.SUCCESS) {
		throw new Error(`UpdateOffsetsInTransaction failed: ${response.operation?.status}`)
	}

	dbg.log('updateOffsetsInTransaction completed successfully')
}
