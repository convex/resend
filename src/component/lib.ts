import { v } from "convex/values";
import {
  internalAction,
  mutation,
  type MutationCtx,
  query,
  internalQuery,
  type ActionCtx,
} from "./_generated/server.js";
import { Workpool } from "@convex-dev/workpool";
import { RateLimiter } from "@convex-dev/rate-limiter";
import { api, components, internal } from "./_generated/api.js";
import { internalMutation } from "./_generated/server.js";
import { type Id, type Doc } from "./_generated/dataModel.js";
import {
  ACCEPTED_EVENT_TYPES,
  type RuntimeConfig,
  vEmailEvent,
  vOptions,
  vStatus,
  vTemplate,
} from "./shared.js";
import type { FunctionHandle } from "convex/server";
import type { EmailEvent } from "./shared.js";
import { isDeepEqual } from "remeda";
import schema from "./schema.js";
import { omit } from "convex-helpers";
import { parse } from "convex-helpers/validators";
import { assertExhaustive, attemptToParse } from "./utils.js";

// Move some of these to options? TODO
const SEGMENT_MS = 125;
const BASE_BATCH_DELAY = 1000;
const BATCH_SIZE = 100;
const EMAIL_POOL_SIZE = 4;
const CALLBACK_POOL_SIZE = 4;
const RESEND_ONE_CALL_EVERY_MS = 600; // Half the stated limit, but it keeps us sane.
const FINALIZED_EMAIL_RETENTION_MS = 1000 * 60 * 60 * 24 * 7; // 7 days
const FINALIZED_EPOCH = Number.MAX_SAFE_INTEGER;
const ABANDONED_EMAIL_RETENTION_MS = 1000 * 60 * 60 * 24 * 30; // 30 days

const RESEND_TEST_EMAILS = ["delivered", "bounced", "complained"];
const EMAIL_REFERENCE_TAG = "convex_batch_reference";
const INTERNAL_SUBMISSION_FIELDS = [
  "batchSubmission",
  "batchSize",
  "batchReferenceToken",
  "submissionUnconfirmed",
] as const;

function isTestEmail(email: string) {
  const [prefix, domain] = email.split("@");
  if (domain !== "resend.dev") {
    return false;
  }
  for (const testEmail of RESEND_TEST_EMAILS) {
    if (prefix === testEmail) {
      return true;
    }
    if (prefix.startsWith(testEmail + "+")) {
      return true;
    }
  }
  return false;
}
const PERMANENT_ERROR_CODES = new Set([
  400, 401 /* 402 not included - unclear spec */, 403, 404, 405, 406, 407,
  /* 409 not included - conflict may work on retry */
  410, 411 /* 412 not included - precondition may have changed? */, 413, 414,
  415, 416 /* 417, not included - expectation may be met later? */, 418, 421,
  422 /*423, 424, 425, may change over time */, 426, 427,
  428 /* 429, explicitly asked to retry */, 431 /* 451, laws change? */,
]);

// We break the emails into segments to avoid contention on new emails being inserted.
function getSegment(now: number) {
  return Math.floor(now / SEGMENT_MS);
}

// Four threads is more than enough, especially given the low rate limiting.
const emailPool = new Workpool(components.emailWorkpool, {
  maxParallelism: EMAIL_POOL_SIZE,
});

// We need to run callbacks in a separate pool so we don't tie up too many threads.
const callbackPool = new Workpool(components.callbackWorkpool, {
  maxParallelism: CALLBACK_POOL_SIZE,
});

// We rate limit our calls to the Resend API.
// FUTURE -- make this rate configurable if an account ups its sending rate with Resend.
const resendApiRateLimiter = new RateLimiter(components.rateLimiter, {
  resendApi: {
    kind: "fixed window",
    period: RESEND_ONE_CALL_EVERY_MS,
    rate: 1,
  },
});

// Periodic background job to clean up old emails that have already
// been delivered, bounced, what have you.
export const cleanupOldEmails = mutation({
  args: { olderThan: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    const BATCH_SIZE = 100;
    const olderThan = args.olderThan ?? FINALIZED_EMAIL_RETENTION_MS;
    const oldAndDone = await ctx.db
      .query("emails")
      .withIndex("by_finalizedAt", (q) =>
        q.lt("finalizedAt", Date.now() - olderThan),
      )
      .take(BATCH_SIZE);
    for (const email of oldAndDone) {
      await cleanupEmail(ctx, email);
    }
    if (oldAndDone.length > 0) {
      console.log(`Cleaned up ${oldAndDone.length} emails`);
    }
    if (oldAndDone.length === BATCH_SIZE) {
      await ctx.scheduler.runAfter(0, api.lib.cleanupOldEmails, {
        olderThan,
      });
    }
  },
});

// Enqueue an email to be send.  A background job will grab batches
// of emails and enqueue them to be sent by the workpool.
export const sendEmail = mutation({
  args: {
    options: vOptions,
    from: v.string(),
    to: v.array(v.string()),
    cc: v.optional(v.array(v.string())),
    bcc: v.optional(v.array(v.string())),
    subject: v.optional(v.string()),
    html: v.optional(v.string()),
    text: v.optional(v.string()),
    template: v.optional(vTemplate),
    replyTo: v.optional(v.array(v.string())),
    headers: v.optional(
      v.array(
        v.object({
          name: v.string(),
          value: v.string(),
        }),
      ),
    ),
    idempotencyKey: v.optional(v.string()),
  },
  returns: v.id("emails"),
  handler: async (ctx, args) => {
    // If the caller supplied an idempotency key, dedupe on it: if we've already
    // enqueued an email with this key, return the existing email's id rather than
    // enqueueing a duplicate. Mutations are transactional, so the index read and
    // the subsequent insert are atomic with respect to concurrent enqueues.
    if (args.idempotencyKey !== undefined) {
      const existing = await ctx.db
        .query("emails")
        .withIndex("by_idempotencyKey", (q) =>
          q.eq("idempotencyKey", args.idempotencyKey),
        )
        .first();
      if (existing) {
        return existing._id;
      }
    }

    // We only allow test emails in test mode.
    if (args.options.testMode) {
      for (const to of [...args.to, ...(args.cc ?? []), ...(args.bcc ?? [])]) {
        if (!isTestEmail(to)) {
          throw new Error(
            `Test mode is enabled, but email address is not a valid resend test address. Did you want to set testMode: false in your ResendOptions?`,
          );
        }
      }
    }
    // We require either html/text or a template to be provided.
    const hasContent = args.html !== undefined || args.text !== undefined;
    const hasTemplate = args.template?.id !== undefined;

    if (!hasContent && !hasTemplate) {
      throw new Error("Either html/text or template must be provided");
    }
    if (hasContent && hasTemplate) {
      throw new Error("Cannot provide both html/text and template");
    }
    if (!hasTemplate && args.subject === undefined) {
      throw new Error("Subject is required when not using a template");
    }

    // Store the text/html into separate records to keep things fast and memory low when we work with email batches.
    let htmlContentId: Id<"content"> | undefined;
    if (args.html !== undefined) {
      const contentId = await ctx.db.insert("content", {
        content: new TextEncoder().encode(args.html).buffer,
        mimeType: "text/html",
      });
      htmlContentId = contentId;
    }

    let textContentId: Id<"content"> | undefined;
    if (args.text !== undefined) {
      const contentId = await ctx.db.insert("content", {
        content: new TextEncoder().encode(args.text).buffer,
        mimeType: "text/plain",
      });
      textContentId = contentId;
    }

    // This is the "send requested" segment.
    const segment = getSegment(Date.now());

    // Okay, we're ready to insert the email into the database, waiting for a background job to enqueue it.
    const emailId = await ctx.db.insert("emails", {
      from: args.from,
      to: args.to,
      cc: args.cc,
      bcc: args.bcc,
      subject: args.subject,
      html: htmlContentId,
      text: textContentId,
      template: args.template,
      headers: args.headers,
      idempotencyKey: args.idempotencyKey,
      segment,
      status: "waiting",
      batchSubmission: "pending",
      bounced: false,
      complained: false,
      failed: false,
      deliveryDelayed: false,
      opened: false,
      clicked: false,
      replyTo: args.replyTo ?? [],
      finalizedAt: FINALIZED_EPOCH,
    });

    // Ensure there is a worker running to grab batches of emails.
    await scheduleBatchRun(ctx, args.options);
    return emailId;
  },
});

export const createManualEmail = mutation({
  args: {
    from: v.string(),
    to: v.union(v.array(v.string()), v.string()),
    cc: v.optional(v.union(v.array(v.string()), v.string())),
    bcc: v.optional(v.union(v.array(v.string()), v.string())),
    subject: v.string(),
    replyTo: v.optional(v.array(v.string())),
    headers: v.optional(
      v.array(
        v.object({
          name: v.string(),
          value: v.string(),
        }),
      ),
    ),
  },
  returns: v.id("emails"),
  handler: async (ctx, args) => {
    const emailId = await ctx.db.insert("emails", {
      from: args.from,
      to: Array.isArray(args.to) ? args.to : [args.to],
      cc: toArray(args.cc),
      bcc: toArray(args.bcc),
      subject: args.subject,
      headers: args.headers,
      segment: Infinity,
      status: "queued",
      bounced: false,
      complained: false,
      failed: false,
      deliveryDelayed: false,
      opened: false,
      clicked: false,
      replyTo: args.replyTo ?? [],
      finalizedAt: FINALIZED_EPOCH,
    });
    return emailId;
  },
});

function toArray<T>(value: T | T[] | undefined): T[] | undefined {
  if (value === undefined) return undefined;
  return Array.isArray(value) ? value : [value];
}

export const updateManualEmail = mutation({
  args: {
    emailId: v.id("emails"),
    status: vStatus,
    resendId: v.optional(v.string()),
    errorMessage: v.optional(v.string()),
  },
  returns: v.union(v.id("emails"), v.null()),
  handler: async (ctx, args) => {
    const email = await ctx.db.get("emails", args.emailId);
    if (args.resendId) {
      const bound = await ctx.db
        .query("emails")
        .withIndex("by_resendId", (q) => q.eq("resendId", args.resendId))
        .first();
      if (bound?._id === args.emailId) return bound._id;
      if (bound) {
        // Existing applications may already pass a stable provider idempotency key
        // from the manual callback. Return the canonical row for that same send.
        const sameAddresses = (
          first?: string | string[],
          second?: string | string[],
        ) =>
          JSON.stringify((toArray(first) ?? []).slice().sort()) ===
          JSON.stringify((toArray(second) ?? []).slice().sort());
        if (
          args.status === "sent" &&
          email?.segment === Infinity &&
          email.status === "queued" &&
          !email.resendId &&
          bound.segment === Infinity &&
          email.from === bound.from &&
          email.subject === bound.subject &&
          sameAddresses(email.to, bound.to) &&
          sameAddresses(email.cc, bound.cc) &&
          sameAddresses(email.bcc, bound.bcc)
        ) {
          await cleanupEmail(ctx, email);
          return bound._id;
        }
        throw new Error(
          "Resend message ID is already assigned to another email",
        );
      }
    }
    const finalizedAt =
      args.status === "failed" || args.status === "cancelled"
        ? Date.now()
        : undefined;
    await ctx.db.patch("emails", args.emailId, {
      status: args.status,
      resendId: args.resendId,
      errorMessage: args.errorMessage,
      ...(finalizedAt ? { finalizedAt } : {}),
    });
    return null;
  },
});

// Cancel an email that has not been sent yet. The worker will ignore it
// within whatever batch it is in.
export const cancelEmail = mutation({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const email = await ctx.db.get("emails", args.emailId);
    if (!email) {
      throw new Error("Email not found");
    }
    if (
      (email.status !== "waiting" && email.status !== "queued") ||
      email.batchSubmission === "started" ||
      email.batchSubmission === "legacy_started"
    ) {
      throw new Error("Email has already been sent");
    }
    await ctx.db.patch("emails", args.emailId, {
      status: "cancelled",
      finalizedAt: Date.now(),
    });
  },
});

// Get the status of an email.
export const getStatus = query({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.union(
    v.object({
      status: vStatus,
      errorMessage: v.union(v.string(), v.null()),
      bounced: v.boolean(),
      complained: v.boolean(),
      failed: v.boolean(),
      deliveryDelayed: v.boolean(),
      opened: v.boolean(),
      clicked: v.boolean(),
    }),
    v.null(),
  ),
  handler: async (ctx, args) => {
    const email = await ctx.db.get("emails", args.emailId);
    if (!email) {
      return null;
    }
    return {
      status: email.status,
      errorMessage: email.errorMessage ?? null,
      bounced: email.bounced ?? false,
      complained: email.complained,
      failed: email.failed ?? false,
      deliveryDelayed: email.deliveryDelayed ?? false,
      opened: email.opened,
      clicked: email.clicked ?? false,
    };
  },
});

// Get the entire email.
export const get = query({
  args: {
    emailId: v.id("emails"),
  },
  returns: v.union(
    v.object({
      ...omit(schema.tables.emails.validator.fields, [
        "html",
        "text",
        ...INTERNAL_SUBMISSION_FIELDS,
      ]),
      createdAt: v.number(),
      html: v.optional(v.string()),
      text: v.optional(v.string()),
      to: v.array(v.string()),
    }),
    v.null(),
  ),
  handler: async (ctx, args) => {
    const email = await ctx.db.get("emails", args.emailId);
    if (!email) {
      return null;
    }
    const html = email.html
      ? new TextDecoder().decode(
          (await ctx.db.get("content", email.html))?.content,
        )
      : undefined;
    const text = email.text
      ? new TextDecoder().decode(
          (await ctx.db.get("content", email.text))?.content,
        )
      : undefined;
    return {
      ...omit(email, [
        "html",
        "text",
        "_id",
        "_creationTime",
        ...INTERNAL_SUBMISSION_FIELDS,
      ]),
      createdAt: email._creationTime,
      html,
      text,
      to: Array.isArray(email.to) ? email.to : [email.to],
    };
  },
});

// Ensure there is a worker running to grab batches of emails.
async function scheduleBatchRun(ctx: MutationCtx, options: RuntimeConfig) {
  // Update the last options if they've changed.
  const lastOptions = await ctx.db.query("lastOptions").unique();
  if (!lastOptions) {
    await ctx.db.insert("lastOptions", {
      options,
    });
  } else if (!isDeepEqual(lastOptions.options, options)) {
    await ctx.db.replace("lastOptions", lastOptions._id, {
      options,
    });
  }

  // Check if there is already a worker running.
  const existing = await ctx.db.query("nextBatchRun").unique();

  // Is there already a worker running?
  if (existing) {
    return;
  }

  // No worker running? Schedule one.
  const runId = await ctx.scheduler.runAfter(
    BASE_BATCH_DELAY,
    internal.lib.makeBatch,
    { reloop: false, segment: getSegment(Date.now() + BASE_BATCH_DELAY) },
  );

  // Insert the new worker to reserve exactly one running.
  await ctx.db.insert("nextBatchRun", {
    runId,
  });
}

// A background job that grabs batches of emails and enqueues them to be sent by the workpool.
export const makeBatch = internalMutation({
  args: { reloop: v.boolean(), segment: v.number() },
  returns: v.null(),
  handler: async (ctx, args) => {
    // Get the API key for the worker.
    const lastOptions = await ctx.db.query("lastOptions").unique();
    if (!lastOptions) {
      throw new Error("No last options found -- invariant");
    }
    const options = lastOptions.options;

    // Grab the batch of emails to send.
    const emails = await ctx.db
      .query("emails")
      .withIndex("by_status_segment", (q) =>
        // We scan earlier than two segments ago to avoid contention between new email insertions and batch creation.
        q.eq("status", "waiting").lte("segment", args.segment - 2),
      )
      .take(BATCH_SIZE);

    // If we have no emails, or we have a short batch on a reloop,
    // let's delay working for now.
    if (emails.length === 0 || (args.reloop && emails.length < BATCH_SIZE)) {
      return reschedule(ctx, emails.length > 0);
    }

    console.log(`Making a batch of ${emails.length} emails`);

    // Mark the emails as queued.
    for (const email of emails) {
      await ctx.db.patch("emails", email._id, {
        status: "queued",
        batchSubmission: "pending",
      });
    }

    // Okay, let's calculate rate limiting as best we can globally in this distributed system.
    const delay = await getDelay(ctx);

    // Give the batch to the workpool! It will call the Resend batch API
    // in a durable background action.
    await emailPool.enqueueAction(
      ctx,
      internal.lib.callResendAPIWithBatch,
      {
        apiKey: options.apiKey,
        emails: emails.map((e) => e._id),
      },
      {
        retry: {
          maxAttempts: options.retryAttempts,
          initialBackoffMs: options.initialBackoffMs,
          base: 2,
        },
        runAfter: delay,
        context: { emailIds: emails.map((e) => e._id) },
        onComplete: internal.lib.onEmailComplete,
      },
    );

    // Let's go around again until there are no more batches to make in this particular segment range.
    await ctx.scheduler.runAfter(0, internal.lib.makeBatch, {
      reloop: true,
      segment: args.segment,
    });
  },
});

// If there are no more emails to send in this segment range, we need to check to see if there are any
// emails in newer segments and so we should sleep for a bit before trying to make batches again.
// If the table is empty, we need to stop the worker and idle the system until a new email is inserted.
async function reschedule(ctx: MutationCtx, emailsLeft: boolean) {
  emailsLeft =
    emailsLeft ||
    (await ctx.db
      .query("emails")
      .withIndex("by_status_segment", (q) => q.eq("status", "waiting"))
      .first()) !== null;

  if (!emailsLeft) {
    // No next email yet?
    const batchRun = await ctx.db.query("nextBatchRun").unique();
    if (!batchRun) {
      throw new Error("No batch run found -- invariant");
    }
    await ctx.db.delete("nextBatchRun", batchRun._id);
  } else {
    const segment = getSegment(Date.now() + BASE_BATCH_DELAY);
    await ctx.scheduler.runAfter(BASE_BATCH_DELAY, internal.lib.makeBatch, {
      reloop: false,
      segment,
    });
  }
}

// Helper to fetch content. We'll use batch apis here to avoid lots of action->query calls.
async function getAllContent(
  ctx: ActionCtx,
  contentIds: Id<"content">[],
): Promise<Map<Id<"content">, string>> {
  const docs = await ctx.runQuery(internal.lib.getAllContentByIds, {
    contentIds,
  });
  return new Map(docs.map((doc) => [doc.id, doc.content]));
}

const vBatchReturns = v.union(
  v.null(),
  v.object({
    emailIds: v.array(v.id("emails")),
    resendIds: v.array(v.string()),
  }),
);

// Okay, finally! Let's call the Resend API with the batch of emails.
export const callResendAPIWithBatch = internalAction({
  args: {
    apiKey: v.string(),
    emails: v.array(v.id("emails")),
  },
  returns: vBatchReturns,
  handler: async (
    ctx,
    args,
  ): Promise<{ emailIds: Id<"emails">[]; resendIds: string[] } | null> => {
    // Construct the JSON payload for the Resend API from all the database values.
    const { emailIds, firstAttempt } = await ctx.runMutation(
      internal.lib.beginBatchSubmission,
      { emailIds: args.emails, referenceToken: crypto.randomUUID() },
    );
    if (emailIds.length === 0) {
      // No emails to send.
      console.log("No emails to send in batch. All were cancelled or failed.");
      return null;
    }

    const body = await createResendBatchPayload(ctx, emailIds);

    // Make API call
    const response = await fetch("https://api.resend.com/emails/batch", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${args.apiKey}`,
        "Content-Type": "application/json",
        "Idempotency-Key": args.emails[0].toString(),
      },
      body,
    });
    if (!response.ok) {
      if (firstAttempt && PERMANENT_ERROR_CODES.has(response.status)) {
        const errorMessage = `Resend API error: ${response.status} ${response.statusText} ${await response.text()}`;
        console.error(
          JSON.stringify({
            status: "permanent_failure",
            httpStatus: response.status,
            httpStatusText: response.statusText,
            errorMessage,
            emailIds,
          }),
        );
        await ctx.runMutation(internal.lib.markEmailsFailed, {
          emailIds,
          errorMessage,
        });
        return null;
      }
      throw new Error(`Resend API error: ${await response.text()}`);
    } else {
      const data = await response.json();
      if (
        !Array.isArray(data.data) ||
        data.data.length !== emailIds.length ||
        data.data.some(
          (entry: { id?: unknown } | null) =>
            typeof entry?.id !== "string" || !entry.id,
        )
      ) {
        throw new Error("Resend did not acknowledge every email in the batch");
      }
      const resendIds = data.data.map((d: { id: string }) => d.id);
      if (new Set(resendIds).size !== resendIds.length) {
        throw new Error("Resend returned duplicate message IDs in the batch");
      }
      return { emailIds, resendIds };
    }
  },
});

export const beginBatchSubmission = internalMutation({
  args: { emailIds: v.array(v.id("emails")), referenceToken: v.string() },
  returns: v.object({
    emailIds: v.array(v.id("emails")),
    firstAttempt: v.boolean(),
  }),
  handler: async (ctx, { emailIds, referenceToken }) => {
    const rows = await Promise.all(
      emailIds.map((id) => ctx.db.get("emails", id)),
    );
    const emails = rows.filter(
      (email): email is Doc<"emails"> =>
        email !== null &&
        (email.status === "queued" ||
          email.batchSubmission === "started" ||
          email.batchSubmission === "legacy_started"),
    );
    if (
      emails.some((email) => email.batchSubmission === undefined) &&
      rows.some((email) => email === null || email.status === "cancelled")
    ) {
      // A batch first seen after an upgrade has no membership snapshot.
      throw new Error("Cannot reconstruct an existing Resend batch");
    }
    const batchSize = emails.find(
      (email) => email.batchSize !== undefined,
    )?.batchSize;
    if (batchSize !== undefined && batchSize !== emails.length) {
      throw new Error(
        "Cannot change the contents of an attempted Resend batch",
      );
    }
    const firstAttempt = emails.every(
      (email) => email.batchSubmission === "pending",
    );
    for (const email of emails) {
      if (
        email.batchSubmission === "pending" ||
        email.batchSubmission === undefined
      ) {
        // Historic batches may already have reached Resend. Preserve their
        // original payload and treat any subsequent rejection as unconfirmed.
        await ctx.db.patch("emails", email._id, {
          batchSubmission:
            email.batchSubmission === "pending" ? "started" : "legacy_started",
          batchSize: emails.length,
          batchReferenceToken:
            email.batchSubmission === "pending" ? referenceToken : undefined,
        });
      }
    }
    return { emailIds: emails.map((email) => email._id), firstAttempt };
  },
});

export const markEmailsFailed = internalMutation({
  args: {
    emailIds: v.array(v.id("emails")),
    errorMessage: v.string(),
  },
  returns: v.null(),
  handler: markEmailsFailedHandler,
});

async function markEmailsFailedHandler(
  ctx: MutationCtx,
  args: {
    emailIds: Id<"emails">[];
    errorMessage: string;
  },
) {
  await Promise.all(
    args.emailIds.map(async (emailId) => {
      const email = await ctx.db.get("emails", emailId);
      if (!email || email.status !== "queued") {
        return;
      }
      await ctx.db.patch("emails", emailId, {
        status: "failed",
        batchSubmission: undefined,
        batchSize: undefined,
        batchReferenceToken: undefined,
        errorMessage: args.errorMessage,
        finalizedAt: Date.now(),
      });
    }),
  );
}

export const onEmailComplete = emailPool.defineOnComplete({
  context: v.object({
    emailIds: v.array(v.id("emails")),
  }),
  handler: async (ctx, args) => {
    if (args.result.kind === "success") {
      const result = parse(vBatchReturns, args.result.returnValue);
      if (result === null) {
        return;
      }
      const { emailIds, resendIds } = result;
      // A completion produced before an upgrade may bypass action validation.
      if (
        emailIds.length !== resendIds.length ||
        new Set(resendIds).size !== resendIds.length
      ) {
        for (const id of emailIds) {
          const email = await ctx.db.get("emails", id);
          if (email?.status === "queued" && !email.resendId)
            await markSubmissionUnconfirmed(ctx, id);
        }
        return;
      }
      for (const [index, emailId] of emailIds.entries()) {
        const email = await ctx.db.get("emails", emailId);
        if (!email) continue;
        const resendId = resendIds[index];
        const bound = await ctx.db
          .query("emails")
          .withIndex("by_resendId", (q) => q.eq("resendId", resendId))
          .first();
        if (
          (bound && bound._id !== emailId) ||
          (email.resendId && email.resendId !== resendId)
        ) {
          console.warn("Conflicting Resend message ID", { emailId, resendId });
          if (email.status === "queued")
            await markSubmissionUnconfirmed(ctx, emailId);
          continue;
        }
        if (email.status === "queued" || email.submissionUnconfirmed) {
          await ctx.db.patch("emails", emailId, {
            status: "sent",
            resendId,
            errorMessage: undefined,
            submissionUnconfirmed: undefined,
            finalizedAt: FINALIZED_EPOCH,
          });
        }
      }
    } else {
      for (const emailId of args.context.emailIds) {
        const email = await ctx.db.get("emails", emailId);
        if (email?.status === "queued") {
          if (
            args.result.kind === "canceled" &&
            email.batchSubmission === "pending"
          ) {
            await ctx.db.patch("emails", emailId, {
              status: "cancelled",
              errorMessage: "Resend API batch job was cancelled",
              finalizedAt: Date.now(),
            });
            continue;
          }
          await markSubmissionUnconfirmed(ctx, emailId);
        }
      }
    }
  },
});

async function markSubmissionUnconfirmed(
  ctx: MutationCtx,
  emailId: Id<"emails">,
) {
  await ctx.db.patch("emails", emailId, {
    status: "failed",
    errorMessage: "Resend submission was not confirmed",
    submissionUnconfirmed: true,
    finalizedAt: Date.now(),
  });
}

// Helper to create the JSON payload for the Resend API.
async function createResendBatchPayload(
  ctx: ActionCtx,
  emailIds: Id<"emails">[],
): Promise<string> {
  // Fetch emails from database.
  const emails = await ctx.runQuery(internal.lib.getEmailsByIds, {
    emailIds,
  });
  if (emails.length !== emailIds.length) {
    throw new Error("Cannot retry a Resend batch with missing emails");
  }
  // Fetch body content from database.
  const contentMap = await getAllContent(
    ctx,
    emails
      .flatMap((e) => [e.html, e.text])
      .filter((id): id is Id<"content"> => id !== undefined),
  );

  // Build payload for resend API.
  const batchPayload = emails.map((email: Doc<"emails">) => {
    const payload: Record<string, unknown> = {
      from: email.from,
      tags: email.batchReferenceToken
        ? [
            {
              name: EMAIL_REFERENCE_TAG,
              value: `${email._id}_${email.batchReferenceToken}`,
            },
          ]
        : undefined,
      to: Array.isArray(email.to) ? email.to : [email.to],
      bcc: email.bcc,
      cc: email.cc,
      reply_to: email.replyTo ? email.replyTo : undefined,
      headers: email.headers
        ? Object.fromEntries(
            email.headers.map((h: { name: string; value: string }) => [
              h.name,
              h.value,
            ]),
          )
        : undefined,
    };

    // Add either template or traditional content
    if (email.template) {
      payload.template = email.template;
      // Subject can be optionally provided with templates
      if (email.subject) {
        payload.subject = email.subject;
      }
    } else {
      payload.subject = email.subject;
      payload.html = email.html ? contentMap.get(email.html) : undefined;
      payload.text = email.text ? contentMap.get(email.text) : undefined;
    }

    return payload;
  });

  return JSON.stringify(batchPayload);
}

const FIXED_WINDOW_DELAY = 100;
async function getDelay(ctx: MutationCtx): Promise<number> {
  const limit = await resendApiRateLimiter.limit(ctx, "resendApi", {
    reserve: true,
  });
  //console.log(`RL: ${limit.ok} ${limit.retryAfter}`);
  const jitter = Math.random() * FIXED_WINDOW_DELAY;
  return limit.retryAfter ? limit.retryAfter + jitter : 0;
}

// Helper to fetch content by id. We'll use batch apis here to avoid lots of action->query calls.
export const getAllContentByIds = internalQuery({
  args: { contentIds: v.array(v.id("content")) },
  returns: v.array(v.object({ id: v.id("content"), content: v.string() })),
  handler: async (ctx, args) => {
    const contentMap = [];
    const promises = [];
    for (const contentId of args.contentIds) {
      promises.push(ctx.db.get("content", contentId));
    }
    const docs = await Promise.all(promises);
    for (const doc of docs) {
      if (!doc) throw new Error("Content not found -- invariant");
      contentMap.push({
        id: doc._id,
        content: new TextDecoder().decode(doc.content),
      });
    }
    return contentMap;
  },
});

// Helper to fetch emails by id. We'll use batch apis here to avoid lots of action->query calls.
export const getEmailsByIds = internalQuery({
  args: { emailIds: v.array(v.id("emails")) },
  handler: async (ctx, args) => {
    const emails = await Promise.all(
      args.emailIds.map((id) => ctx.db.get("emails", id)),
    );

    // Some emails might be missing b/c they were cancelled long ago and already
    // cleaned up because the retention period has passed.
    return emails.filter((e): e is Doc<"emails"> => e !== null);
  },
});

// Helper to fetch an email by resendId. This is used by the webhook handler.
// Resend gives us *their* id back, no ours. We'll use the index to find it.
export const getEmailByResendId = internalQuery({
  args: { resendId: v.string() },
  handler: async (ctx, args) => {
    const email = await ctx.db
      .query("emails")
      .withIndex("by_resendId", (q) => q.eq("resendId", args.resendId))
      .first();
    if (!email) throw new Error("Email not found for resendId");
    return email;
  },
});

// Compute the updated email record for a given webhook event without writing it.
// Only returns an update if there's a state change to reduce write contention.
function computeEmailUpdateFromEvent(
  email: Doc<"emails">,
  event: EmailEvent,
): Doc<"emails"> | null {
  // Define precedence for statuses; only allow upgrades
  const statusRank: Record<Doc<"emails">["status"], number> = {
    waiting: 0,
    queued: 1,
    sent: 2,
    delivery_delayed: 3,
    delivered: 4,
    bounced: 5,
    failed: 5,
    cancelled: 100, // treat cancelled as terminal
  };

  const currentRank = statusRank[email.status];
  const canUpgradeTo = (next: Doc<"emails">["status"]) => {
    if (email.status === "cancelled") return false;
    return statusRank[next] > currentRank;
  };

  // NOOP -- we do this automatically when we send the email.
  if (event.type == "email.sent") return null;

  if (event.type == "email.clicked") {
    // Only mutate if this is the first click
    if (email.clicked) return null;
    return {
      ...email,
      clicked: true,
    };
  }

  if (event.type == "email.failed") {
    // Only mutate if this is the first failure OR status changes
    const statusWillChange = canUpgradeTo("failed");
    if (!statusWillChange && email.failed) {
      return null; // No state change
    }
    const updated: Doc<"emails"> = {
      ...email,
      failed: true,
    };
    if (statusWillChange) {
      updated.status = "failed";
      updated.finalizedAt = Date.now();
    }
    return updated;
  }

  if (event.type == "email.delivered") {
    if (!canUpgradeTo("delivered")) return null;
    return {
      ...email,
      status: "delivered",
      finalizedAt: Date.now(),
    };
  }

  if (event.type == "email.bounced") {
    // Only mutate if this is the first bounce OR status changes
    const statusWillChange = canUpgradeTo("bounced");
    if (!statusWillChange && email.bounced) {
      return null; // No state change
    }
    const updated: Doc<"emails"> = {
      ...email,
      errorMessage: event.data.bounce?.message,
      bounced: true,
    };
    if (statusWillChange) {
      updated.status = "bounced";
      updated.finalizedAt = Date.now();
    }
    return updated;
  }

  if (event.type == "email.delivery_delayed") {
    // Only mutate if this is the first delay OR status changes
    const statusWillChange = canUpgradeTo("delivery_delayed");
    if (!statusWillChange && email.deliveryDelayed) {
      return null; // No state change
    }
    const updated: Doc<"emails"> = {
      ...email,
      deliveryDelayed: true,
    };
    if (statusWillChange) {
      updated.status = "delivery_delayed";
    }
    return updated;
  }

  if (event.type == "email.complained") {
    // Only mutate if this is the first complaint
    if (email.complained) return null;
    return {
      ...email,
      complained: true,
      finalizedAt:
        email.finalizedAt === FINALIZED_EPOCH ? Date.now() : email.finalizedAt,
    };
  }

  if (event.type == "email.opened") {
    // Only mutate if this is the first open
    if (email.opened) return null;
    return {
      ...email,
      opened: true,
    };
  }

  assertExhaustive(event);
  return null;
}

// Handle a webhook event. Mostly we just update the email status.
export const handleEmailEvent = mutation({
  args: {
    event: v.any(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    // Event can be anything, so we need to parse it.
    // this will also strip out anything that shouldnt be there.
    const result = attemptToParse(vEmailEvent, args.event);
    if (result.kind === "error") {
      console.warn(
        `Invalid email event received. You might want to to exclude this event from your Resend webhook settings in the Resend dashboard. ${result.error}.`,
      );
      return;
    }

    const event = result.data;
    const tags = event.data.tags;
    const reference = Array.isArray(tags)
      ? tags.find((tag) => tag.name === EMAIL_REFERENCE_TAG)?.value
      : tags?.[EMAIL_REFERENCE_TAG];

    let email = await ctx.db
      .query("emails")
      .withIndex("by_resendId", (q) => q.eq("resendId", event.data.email_id))
      // Older manual retries could leave multiple rows for one provider ID.
      .first();

    const separator = reference?.indexOf("_") ?? -1;
    const referencedId =
      reference && separator > 0
        ? ctx.db.normalizeId("emails", reference.slice(0, separator))
        : null;
    const candidate = referencedId
      ? await ctx.db.get("emails", referencedId)
      : null;
    const submitted =
      candidate?.batchReferenceToken &&
      reference === `${candidate._id}_${candidate.batchReferenceToken}`
        ? candidate
        : null;

    if (
      email?.batchReferenceToken &&
      submitted &&
      submitted._id !== email._id
    ) {
      console.warn("Conflicting Resend email reference", {
        resendId: event.data.email_id,
      });
      return;
    }
    if (!email) {
      if (
        submitted &&
        (submitted.status === "queued" || submitted.submissionUnconfirmed) &&
        submitted.batchSubmission === "started" &&
        submitted.resendId === undefined &&
        matchesSubmissionEnvelope(submitted, event)
      ) {
        const accepted = {
          status: "sent" as const,
          resendId: event.data.email_id,
          errorMessage: undefined,
          submissionUnconfirmed: undefined,
          finalizedAt: FINALIZED_EPOCH,
        };
        await ctx.db.patch("emails", submitted._id, accepted);
        email = { ...submitted, ...accepted };
      }
    }

    if (!email) {
      console.info(
        `Email not found for resendId: ${event.data.email_id}, ignoring...`,
      );
      return;
    }

    // Record delivery-related events for auditing/analytics (now including opened/clicked)
    if (ACCEPTED_EVENT_TYPES.includes(event.type)) {
      await ctx.db.insert("deliveryEvents", {
        emailId: email._id,
        resendId: event.data.email_id,
        eventType: event.type,
        createdAt: event.created_at,
        message:
          event.type === "email.bounced"
            ? event.data.bounce?.message
            : event.type === "email.failed"
              ? event.data.failed?.reason
              : undefined,
      });
    }

    // Apply the event directly to update email state if needed
    const updated = computeEmailUpdateFromEvent(email, event);
    if (updated) {
      await ctx.db.replace("emails", email._id, updated);
    }

    // Internal correlation tags are reserved for the component.
    if (tags) {
      if (Array.isArray(tags)) {
        event.data.tags = tags.filter(
          ({ name }) => name !== EMAIL_REFERENCE_TAG,
        );
      } else {
        const publicTags = { ...tags };
        delete publicTags[EMAIL_REFERENCE_TAG];
        event.data.tags = publicTags;
      }
    }
    await enqueueCallbackIfExists(ctx, email, event);
  },
});

function matchesSubmissionEnvelope(email: Doc<"emails">, event: EmailEvent) {
  const address = (value: string) =>
    (value.trim().match(/<([^<>]+)>$/)?.[1] ?? value).trim().toLowerCase();
  const addresses = (value: string | string[]) =>
    (Array.isArray(value) ? value : [value]).map(address);
  const sentTo = new Set(
    addresses(email.to).concat(
      addresses(email.cc ?? []),
      addresses(email.bcc ?? []),
    ),
  );
  const eventTo = addresses(event.data.to).filter(Boolean);
  // Resend may report a delivery outcome for just one recipient of a send.
  return (
    addresses(event.data.from).join(",") === addresses(email.from).join(",") &&
    eventTo.length > 0 &&
    eventTo.every((recipient) => sentTo.has(recipient)) &&
    addresses(event.data.cc ?? []).every((recipient) =>
      addresses(email.cc ?? []).includes(recipient),
    ) &&
    addresses(event.data.bcc ?? []).every((recipient) =>
      addresses(email.bcc ?? []).includes(recipient),
    ) &&
    ((!email.subject && email.template !== undefined) ||
      email.subject === undefined ||
      email.subject === event.data.subject)
  );
}

async function enqueueCallbackIfExists(
  ctx: MutationCtx,
  email: Doc<"emails">,
  event: EmailEvent,
) {
  const lastOptions = await ctx.db.query("lastOptions").unique();
  // lastOptions may not exist if the user only uses sendEmailManually
  if (!lastOptions) {
    return;
  }
  if (lastOptions.options.onEmailEvent) {
    const handle = lastOptions.options.onEmailEvent.fnHandle as FunctionHandle<
      "mutation",
      {
        id: Id<"emails">;
        event: EmailEvent;
      },
      void
    >;
    await callbackPool.enqueueMutation(ctx, handle, {
      id: email._id,
      event: event,
    });
  }
}

async function cleanupEmail(ctx: MutationCtx, email: Doc<"emails">) {
  await ctx.db.delete("emails", email._id);
  if (email.text) {
    await ctx.db.delete("content", email.text);
  }
  if (email.html) {
    await ctx.db.delete("content", email.html);
  }
  const events = await ctx.db
    .query("deliveryEvents")
    .withIndex("by_emailId_eventType", (q) => q.eq("emailId", email._id))
    .collect();
  for (const event of events) {
    await ctx.db.delete("deliveryEvents", event._id);
  }
}

// Periodic background job to clean up old emails that have been abandoned.
// Meaning, even if they're not finalized, we should just get rid of them.
export const cleanupAbandonedEmails = mutation({
  args: { olderThan: v.optional(v.number()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    const olderThan = args.olderThan ?? ABANDONED_EMAIL_RETENTION_MS;
    const oldAndAbandoned = await ctx.db
      .query("emails")
      .withIndex("by_creation_time", (q) =>
        q.lt("_creationTime", Date.now() - olderThan),
      )
      .take(500);

    for (const email of oldAndAbandoned) {
      // No webhook to finalize these. We'll just delete them.
      await cleanupEmail(ctx, email);
    }
    if (oldAndAbandoned.length > 0) {
      console.log(`Cleaned up ${oldAndAbandoned.length} emails`);
    }
    if (oldAndAbandoned.length === 500) {
      await ctx.scheduler.runAfter(0, api.lib.cleanupAbandonedEmails, {
        olderThan,
      });
    }
  },
});
