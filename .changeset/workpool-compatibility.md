---
"@convex-dev/resend": patch
---

Support Workpool 0.5.x while retaining compatibility with 0.4.10 and later 0.4.x
releases.

Workpool 0.5 requires Convex 1.47 or later. If upgrading from Workpool earlier
than 0.4.15, first deploy Workpool 0.4.15 and run the following for each Resend
instance, adjusting the paths for custom component names.

```sh
npx convex run --component resend/emailWorkpool migrations:backfillPendingStartIds
npx convex run --component resend/callbackWorkpool migrations:backfillPendingStartIds
```
