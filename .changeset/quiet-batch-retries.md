---
"@convex-dev/resend": patch
---

Preserve batch idempotency and reconcile delivery webhooks when Resend accepts a
batch but the request acknowledgement is lost. An exhausted unconfirmed
submission can be corrected by a later webhook. Once submission starts,
cancellation is refused to preserve the provider's idempotent request body.

A batch request that times out with HTTP 408 retries with the same idempotency
key. Manual sends throw `ManualEmailRecordingError` when Resend accepted the
email but Convex could not record that acceptance. The error includes both IDs
for reconciliation before retrying the send.
