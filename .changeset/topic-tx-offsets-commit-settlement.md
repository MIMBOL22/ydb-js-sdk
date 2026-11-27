---
'@ydbjs/topic': patch
---

Keep transactional reader offsets across reconnects and explicit reader closes, stop delivery before the commit offset snapshot, fail the commit when delivered offsets are non-contiguous instead of sending disjoint ranges, allow only disjoint topic/partition subscriptions per transaction identity and consumer, drain commits accepted during graceful partition stop, forward the transaction abort signal to offset staging, and resolve plain `commit()` only after every partition is acknowledged.
