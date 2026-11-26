import type { TopicPartitionSession } from '../partition-session.js'

/**
 * Offset range delivered to a transactional reader.
 *
 * `start` is inclusive, `end` is exclusive (`[start, end)`), matching
 * `Ydb.Topic.OffsetsRange`.
 */
export type TopicTxOffsetRange = {
	start: bigint
	end: bigint
}

/**
 * Offsets delivered to a transactional reader for one partition.
 *
 * Tracking is keyed by a stable `(topicPath, partitionId)` tuple rather than by
 * `partitionSessionId`, because partition sessions are recreated on every
 * reconnect and may be stopped or ended while their delivered offsets still
 * have to be staged in the transaction.
 *
 * Ranges are kept per partition instead of a single `[firstOffset, lastOffset]`
 * interval: after a seek or a gap the reader must stage only the offsets that
 * were actually delivered, never the gap in between.
 */
/**
 * Maximum number of ranges kept per partition.
 *
 * Sparse delivery (for example repeated seeks) must not grow memory until the
 * transaction ends. Once the cap is exceeded the record is marked as truncated
 * and no further ranges are added; the commit fails with a concise error.
 */
export const MAX_TX_READ_RANGES_PER_PARTITION = 64

export type TopicTxReadOffset = {
	topicPath: string
	partitionId: bigint
	ranges: TopicTxOffsetRange[]
	truncated: boolean
}

export type TopicTxReadOffsets = Map<string, TopicTxReadOffset>

export type TopicTxReadOffsetUpdate = {
	topicPath: string
	partitionId: bigint
	range: TopicTxOffsetRange
}

/**
 * Builds an unambiguous key for a topic partition.
 *
 * A JSON tuple is used instead of a string delimiter so that topic paths
 * containing separators (or NUL bytes) can never collide with another
 * partition. `bigint` is not JSON-serializable, so the partition id is encoded
 * as a decimal string inside the tuple.
 */
export function topicPartitionOffsetKey(topicPath: string, partitionId: bigint): string {
	return JSON.stringify([topicPath, partitionId.toString()])
}

/**
 * Merges the range at `index` with following and preceding ranges that overlap
 * or touch it. Ranges are kept sorted, non-overlapping and adjacency-merged.
 */
function mergeRanges(ranges: TopicTxOffsetRange[], index: number): void {
	let current = ranges[index]!

	// Merge following ranges.
	while (ranges[index + 1] && ranges[index + 1]!.start <= current.end) {
		let next = ranges[index + 1]!
		if (next.end > current.end) {
			current.end = next.end
		}
		ranges.splice(index + 1, 1)
	}

	// Merge with the previous range.
	let previous = ranges[index - 1]
	if (previous && previous.end >= current.start) {
		if (current.end > previous.end) {
			previous.end = current.end
		}
		ranges.splice(index, 1)
	}
}

/**
 * Appends a delivered offset to the partition ranges.
 *
 * The common monotonic case is a fast path. Sparse/out-of-order offsets keep
 * the ranges sorted and non-overlapping: an already covered offset is ignored,
 * a touch at a range boundary extends it, and an offset in a gap is inserted as
 * its own range. Gaps are never bridged because those offsets were not
 * delivered.
 */
function appendOffset(ranges: TopicTxOffsetRange[], offset: bigint): void {
	let last = ranges[ranges.length - 1]

	if (!last) {
		ranges.push({ start: offset, end: offset + 1n })
		return
	}

	if (offset >= last.end) {
		if (offset === last.end) {
			last.end = offset + 1n
		} else {
			ranges.push({ start: offset, end: offset + 1n })
		}
		return
	}

	for (let index = 0; index < ranges.length; index++) {
		let range = ranges[index]!

		if (offset < range.start) {
			if (offset + 1n === range.start) {
				range.start = offset
			} else {
				ranges.splice(index, 0, { start: offset, end: offset + 1n })
			}
			mergeRanges(ranges, index)
			return
		}

		if (offset < range.end) {
			// Already covered by this range.
			return
		}

		if (offset === range.end) {
			range.end = offset + 1n
			mergeRanges(ranges, index)
			return
		}
	}

	// Defensive fallback: the loop above handles every `offset < last.end`.
	ranges.push({ start: offset, end: offset + 1n })
}

/**
 * Records a delivered offset for the `(topicPath, partitionId)` of the session.
 *
 * Semantics are per-range and grow-only. Across reconnects the record survives
 * the partition session, ignores already covered redeliveries and starts a new
 * range when the reader seeks forward.
 */
export function trackTxReadOffset(
	readOffsets: TopicTxReadOffsets,
	partitionSession: TopicPartitionSession,
	offset: bigint
): void {
	let key = topicPartitionOffsetKey(partitionSession.topicPath, partitionSession.partitionId)
	let record = readOffsets.get(key)

	if (!record) {
		record = {
			topicPath: partitionSession.topicPath,
			partitionId: partitionSession.partitionId,
			ranges: [],
			truncated: false,
		}
		readOffsets.set(key, record)
	}

	if (record.truncated) {
		return
	}

	appendOffset(record.ranges, offset)

	if (record.ranges.length > MAX_TX_READ_RANGES_PER_PARTITION) {
		record.truncated = true
		record.ranges.length = 0
	}
}

/**
 * Builds the `updateOffsetsInTransaction` payload for every tracked partition.
 *
 * Partition identity is the stable `(topicPath, partitionId)` pair, so offsets
 * are staged even when the local partition session was stopped or ended (for
 * example after a reconnect or an `EndPartitionSession` message).
 *
 * Fails closed for non-contiguous delivery. YDB accepts a request with disjoint
 * ranges but aborts the transaction commit with `ThereAreGapsInTheOffsetRanges`,
 * so a remaining gap (seek or gap) is reported here and the commit is aborted
 * instead of sending ranges that cannot be committed.
 */
export function collectTxReadOffsetUpdates(
	readOffsets: TopicTxReadOffsets
): TopicTxReadOffsetUpdate[] {
	let updates: TopicTxReadOffsetUpdate[] = []

	for (let record of readOffsets.values()) {
		if (record.truncated) {
			throw new Error(
				`Cannot commit transactional read offsets for topic "${record.topicPath}" ` +
					`partition ${record.partitionId}: more than ` +
					`${MAX_TX_READ_RANGES_PER_PARTITION} non-contiguous ranges were tracked, ` +
					`so the delivered offsets are too sparse to commit; ` +
					`YDB cannot commit disjoint offset ranges`
			)
		}

		if (record.ranges.length === 0) {
			continue
		}

		if (record.ranges.length > 1) {
			let first = record.ranges[0]!
			let second = record.ranges[1]!

			throw new Error(
				`Cannot commit transactional read offsets for topic "${record.topicPath}" ` +
					`partition ${record.partitionId}: delivered offsets are not contiguous ` +
					`(${record.ranges.length} ranges, first gap after offset ` +
					`${first.end - 1n} before ${second.start}); ` +
					`YDB cannot commit disjoint offset ranges`
			)
		}

		let range = record.ranges[0]!
		updates.push({
			topicPath: record.topicPath,
			partitionId: record.partitionId,
			range: {
				start: range.start,
				end: range.end,
			},
		})
	}

	return updates
}
