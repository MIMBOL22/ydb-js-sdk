---
'@ydbjs/query': patch
---

Give every transaction attempt its own TX callable so concurrent transactions on one client cannot overwrite each other's session/transaction IDs or hook registrars.
