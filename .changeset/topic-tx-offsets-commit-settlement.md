---
'@ydbjs/topic': patch
---

Keep transactional reader offsets across reconnects and explicit reader closes, fail the commit when delivered offsets are non-contiguous instead of sending disjoint ranges, allow only one transactional reader per transaction and consumer, and resolve plain `commit()` only after every partition is acknowledged.
