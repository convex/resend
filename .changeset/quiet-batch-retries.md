---
"@convex-dev/resend": patch
---

Preserve batch idempotency and reconcile delivery webhooks when Resend accepts a
batch but the request acknowledgement is lost. An exhausted unconfirmed
submission can be corrected by a later webhook. Once submission starts,
cancellation is refused to preserve the provider's idempotent request body.
