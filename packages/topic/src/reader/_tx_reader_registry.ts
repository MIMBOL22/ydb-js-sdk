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

export type TxReaderTopicInput =
	| string
	| { path: string; partitionIds?: bigint[] }
	| Array<{ path: string; partitionIds?: bigint[] }>

type TxReaderSubscription = {
	path: string
	partitionIds: string[] | null // null means all partitions
}

type TxReaderClaim = TxReaderSubscription & {
	token: symbol
}

export type TxReaderReservation = {
	token: symbol
}

/**
 * Stable registry key for a transaction identity.
 */
export function txReaderIdentityKey(identity: TopicTxIdentity): string {
	return JSON.stringify([identity.sessionId, identity.transactionId])
}

// Claims are scoped by captured transaction identity + consumer. Within a scope
// a reader claims one or more (topic path, partition set) subscriptions;
// readers may coexist while their subscriptions are disjoint.
let txReaderClaims = new Map<string, TxReaderClaim[]>()

function scopeKey(identity: TopicTxIdentity, consumer: string): string {
	return JSON.stringify([identity.sessionId, identity.transactionId, consumer])
}

function normalizeSubscriptions(topic: TxReaderTopicInput): TxReaderSubscription[] {
	let sources =
		typeof topic === 'string' ? [{ path: topic }] : Array.isArray(topic) ? topic : [topic]

	return sources.map((source) => {
		let partitionIds = source.partitionIds
		return {
			path: source.path,
			partitionIds:
				!partitionIds || partitionIds.length === 0
					? null
					: [...new Set(partitionIds.map((id) => id.toString()))].sort(),
		}
	})
}

function overlaps(a: string[] | null, b: string[] | null): boolean {
	// Either side claims all partitions of the topic.
	if (a === null || b === null) {
		return true
	}

	return a.some((id) => b.includes(id))
}

/**
 * Reserves the reader's topic/partition subscriptions for the transaction
 * identity + consumer. Rejects only an actual overlap: different topics, or
 * disjoint partition id sets on the same topic, may coexist.
 *
 * The reservation lasts until the transaction commits or rolls back. Release it
 * with the returned token so a stale hook cannot delete a newer claim.
 */
export function reserveTxReaderSubscriptions(
	identity: TopicTxIdentity,
	consumer: string,
	topic: TxReaderTopicInput
): TxReaderReservation {
	let key = scopeKey(identity, consumer)
	let claims = txReaderClaims.get(key) ?? []
	let subscriptions = normalizeSubscriptions(topic)

	for (let subscription of subscriptions) {
		for (let claim of claims) {
			if (
				claim.path === subscription.path &&
				overlaps(claim.partitionIds, subscription.partitionIds)
			) {
				throw new Error(
					`Transaction already has a topic reader for consumer "${consumer}" ` +
						`overlapping topic "${subscription.path}"; a consumer can read each topic/partition ` +
						`only once per transaction. Configure a single reader with multiple topics instead.`
				)
			}
		}
	}

	let token = Symbol('topic tx reader')
	for (let subscription of subscriptions) {
		claims.push({ token, ...subscription })
	}
	txReaderClaims.set(key, claims)

	return { token }
}

/**
 * Releases the reservation when the transaction ends. Only claims created with
 * the same token are removed.
 */
export function releaseTxReaderSubscriptions(
	identity: TopicTxIdentity,
	consumer: string,
	reservation: TxReaderReservation
): void {
	let key = scopeKey(identity, consumer)
	let claims = txReaderClaims.get(key)

	if (!claims) {
		return
	}

	let remaining = claims.filter((claim) => claim.token !== reservation.token)
	if (remaining.length > 0) {
		txReaderClaims.set(key, remaining)
	} else {
		txReaderClaims.delete(key)
	}
}
