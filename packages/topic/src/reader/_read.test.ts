import * as assert from 'node:assert'
import { Codec } from '@ydbjs/api/topic'
import { test } from 'vitest'

import { TopicPartitionSession } from '../partition-session.js'
import { AsyncPriorityQueue } from '../queue.js'
import { _read } from './_read.js'
import { type TopicTxReadOffsets, topicPartitionOffsetKey } from './_tx_read_offsets.js'

function createResponse(offsets: bigint[], codec: Codec = Codec.UNSPECIFIED) {
	return {
		bytesSize: 1n,
		partitionData: [
			{
				partitionSessionId: 1n,
				batches: [
					{
						producerId: 'producer',
						codec,
						messageData: offsets.map((offset) => ({
							offset,
							seqNo: offset,
							// Distinct payloads let the fake codec fail on a specific message.
							data: new Uint8Array([Number(offset) + 1]),
							uncompressedSize: 1n,
						})),
					},
				],
			},
		],
	} as any
}

function createReadContext(response: any, readOffsets: TopicTxReadOffsets, codecs?: any) {
	let partitionSession = new TopicPartitionSession(1n, 0n, '/test/topic')

	return {
		disposed: false,
		controller: new AbortController(),
		buffer: [response],
		partitionSessions: new Map([[1n, partitionSession]]),
		codecs: codecs ?? new Map(),
		outgoingQueue: new AsyncPriorityQueue(),
		maxBufferSize: 0n,
		freeBufferSize: 0n,
		readOffsets,
		updateFreeBufferSize: () => {},
	}
}

// Offsets are tracked only when the batch is yielded. A failure while decoding
// a later message in the same batch must not make the caller's earlier messages
// (or any message) look delivered.
test('does not track offsets when the batch fails to decode', async () => {
	let decodeCalls = 0
	let codecs = new Map([
		[
			Codec.GZIP,
			{
				codec: Codec.GZIP,
				compress: (payload: Uint8Array) => payload,
				decompress: (payload: Uint8Array) => {
					decodeCalls += 1
					if (decodeCalls === 2) {
						throw new Error('decode failed')
					}
					return payload
				},
			},
		],
	])
	let readOffsets: TopicTxReadOffsets = new Map()
	let ctx = createReadContext(createResponse([0n, 1n], Codec.GZIP), readOffsets, codecs)

	let iterator = _read(ctx as any, { limit: 2 })[Symbol.asyncIterator]()

	await assert.rejects(iterator.next(), /decode failed/)
	assert.strictEqual(readOffsets.size, 0)
})

// Offsets are recorded right before the yield, so messages left in the buffer
// by `limit` are not tracked until they are actually handed to the caller.
test('tracks offsets only for the yielded batch', async () => {
	let readOffsets: TopicTxReadOffsets = new Map()
	let ctx = createReadContext(createResponse([0n, 1n]), readOffsets)

	let iterator = _read(ctx as any, { limit: 1 })[Symbol.asyncIterator]()
	let first = await iterator.next()

	assert.strictEqual(first.done, false)
	assert.strictEqual(first.value?.length, 1)

	let record = readOffsets.get(topicPartitionOffsetKey('/test/topic', 0n))
	assert.ok(record)
	assert.deepStrictEqual(
		record.ranges.map((range) => [range.start, range.end]),
		[[0n, 1n]]
	)
})
