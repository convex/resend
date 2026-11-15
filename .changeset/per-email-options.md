---
"@convex-dev/resend": patch
---

Store the API key, retry settings, and `onEmailEvent` callback with each email,
so multiple callers sharing one Resend component (e.g. Convex Auth and your app)
no longer overwrite each other's options for sending and webhook callbacks.
