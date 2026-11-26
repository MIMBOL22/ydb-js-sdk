/**
 * Identity of a transaction, captured when a transactional reader is created.
 *
 * The query client reuses one callable for every transaction (`Object.assign`
 * overwrites `sessionId`/`transactionId` on it), so the `TX` object itself is
 * not a stable key: concurrent transactions share the same reference.
 */
export type TopicTxIdentity = {
	sessionId: string
	transactionId: string
}

/**
 * Stable registry key for a transaction identity.
 */
export function txReaderIdentityKey(identity: TopicTxIdentity): string {
	return JSON.stringify([identity.sessionId, identity.transactionId])
}

// At most one transactional reader per (transaction identity, consumer):
// deduplication and offset tracking are per reader, so two readers for the same
// transaction and consumer would redeliver messages and stage intersecting
// ranges, which YDB rejects at commit time.
let txReaderConsumers = new Map<string, Set<string>>()

/**
 * Reserves a consumer for the transaction identity.
 *
 * Throws when the same identity already has a reader for this consumer. The
 * claim lasts until the transaction commits or rolls back.
 */
export function reserveTxReaderConsumer(identity: TopicTxIdentity, consumer: string): void {
	let key = txReaderIdentityKey(identity)
	let consumers = txReaderConsumers.get(key)

	if (!consumers) {
		consumers = new Set()
		txReaderConsumers.set(key, consumers)
	}

	if (consumers.has(consumer)) {
		throw new Error(
			`Transaction already has a topic reader for consumer "${consumer}"; ` +
				`a consumer can have only one transactional reader until the transaction ends ` +
				`(commit/rollback). Configure a single reader with multiple topics instead.`
		)
	}

	consumers.add(consumer)
}

/**
 * Releases the consumer claim when the transaction ends.
 */
export function releaseTxReaderConsumer(identity: TopicTxIdentity, consumer: string): void {
	let key = txReaderIdentityKey(identity)
	let consumers = txReaderConsumers.get(key)

	if (!consumers) {
		return
	}

	consumers.delete(consumer)
	if (consumers.size === 0) {
		txReaderConsumers.delete(key)
	}
}
