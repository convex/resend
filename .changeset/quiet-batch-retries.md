---
"@convex-dev/resend": patch
---

Preserve batch idempotency and reconcile delivery webhooks when Resend accepts a
batch but the request acknowledgement is lost. Treat an unconfirmed retry
outcome separately from a known first-attempt rejection.
