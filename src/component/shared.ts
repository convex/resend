import { literals } from "convex-helpers/validators";
import {
  type GenericActionCtx,
  type GenericDataModel,
  type GenericMutationCtx,
  type GenericQueryCtx,
} from "convex/server";
import { type Infer, v, type VString } from "convex/values";

export type EmailId = string & { __isEmailId: true };
export const vEmailId = v.string() as VString<EmailId>;

// Validator for the status of an email.
export const vStatus = v.union(
  v.literal("waiting"),
  v.literal("queued"),
  v.literal("cancelled"),
  v.literal("sent"),
  v.literal("delivered"),
  v.literal("delivery_delayed"),
  v.literal("bounced"),
  v.literal("failed"),
);
export type Status = Infer<typeof vStatus>;

// Validator for template data.
export const vTemplate = v.object({
  id: v.string(),
  variables: v.optional(v.record(v.string(), v.union(v.string(), v.number()))),
});
export type Template = Infer<typeof vTemplate>;

const commonFields = {
  broadcast_id: v.optional(v.string()),
  created_at: v.string(),
  email_id: v.string(),
  from: v.union(v.string(), v.array(v.string())),
  to: v.union(v.string(), v.array(v.string())),
  cc: v.optional(v.union(v.string(), v.array(v.string()))),
  bcc: v.optional(v.union(v.string(), v.array(v.string()))),
  reply_to: v.optional(v.union(v.string(), v.array(v.string()))),
  message_id: v.optional(v.string()), // RFC Message-ID header, e.g. "<abc123@mail.example>"
  headers: v.optional(
    v.array(
      v.object({
        name: v.string(),
        value: v.string(),
      }),
    ),
  ),
  subject: v.string(),
  tags: v.optional(
    v.union(
      v.record(v.string(), v.string()),
      v.array(
        v.object({
          name: v.string(),
          value: v.string(),
        }),
      ),
    ),
  ),
};

// Normalized webhook events coming from Resend.
export const vEmailEvent = v.union(
  v.object({
    type: v.literal("email.sent"),
    created_at: v.string(),
    data: v.object(commonFields),
  }),
  v.object({
    type: v.literal("email.delivered"),
    created_at: v.string(),
    data: v.object(commonFields),
  }),
  v.object({
    type: v.literal("email.delivery_delayed"),
    created_at: v.string(),
    data: v.object(commonFields),
  }),
  v.object({
    type: v.literal("email.complained"),
    created_at: v.string(),
    data: v.object(commonFields),
  }),
  v.object({
    type: v.literal("email.bounced"),
    created_at: v.string(),
    data: v.object({
      ...commonFields,
      bounce: v.object({
        message: v.string(),
        subType: v.string(),
        type: v.string(),
      }),
    }),
  }),
  v.object({
    type: v.literal("email.opened"),
    created_at: v.string(),
    data: v.object({
      ...commonFields,
      open: v.object({
        ipAddress: v.string(),
        timestamp: v.string(),
        userAgent: v.string(),
      }),
    }),
  }),
  v.object({
    type: v.literal("email.clicked"),
    created_at: v.string(),
    data: v.object({
      ...commonFields,
      click: v.object({
        ipAddress: v.string(),
        link: v.string(),
        timestamp: v.string(),
        userAgent: v.string(),
      }),
    }),
  }),
  v.object({
    type: v.literal("email.failed"),
    created_at: v.string(),
    data: v.object({
      ...commonFields,
      failed: v.object({
        reason: v.string(),
      }),
    }),
  }),
);

export const ACCEPTED_EVENT_TYPES = [
  "email.sent",
  "email.delivered",
  "email.bounced",
  "email.complained",
  "email.failed",
  "email.delivery_delayed",
  "email.opened",
  "email.clicked",
] as const;

export const vEventType = v.union(literals(...ACCEPTED_EVENT_TYPES));

export type EmailEvent = Infer<typeof vEmailEvent>;

export const vOnEmailEvent = v.function({
  fnType: "mutation",
  args: { id: vEmailId, event: vEmailEvent },
  returns: v.any(),
});

export const DEFAULT_CONFIG = {
  testMode: true,
  initialBackoffMs: 30000,
  retryAttempts: 5,
};

// The arguments a parent passes in `app.use(resend, { args })`. Each one is
// optional, defaulting to DEFAULT_CONFIG, so `app.use(resend)` keeps working.
export const vInitArgs = {
  testMode: v.optional(v.boolean()),
  initialBackoffMs: v.optional(v.number()),
  retryAttempts: v.optional(v.number()),
  onEmailEvent: v.optional(vOnEmailEvent),
};

export const vConfig = v.object({
  testMode: v.boolean(),
  initialBackoffMs: v.number(),
  retryAttempts: v.number(),
  onEmailEvent: v.optional(vOnEmailEvent),
});
export type Config = Infer<typeof vConfig>;
export type EventEventTypes = EmailEvent["type"];
export type EventEventOfType<T extends EventEventTypes> = Extract<
  EmailEvent,
  { type: T }
>;

/* Type utils follow */

export type QueryCtx = Pick<GenericQueryCtx<GenericDataModel>, "runQuery">;
export type MutationCtx = Pick<
  GenericMutationCtx<GenericDataModel>,
  "runQuery" | "runMutation"
>;
export type ActionCtx = Pick<
  GenericActionCtx<GenericDataModel>,
  "runQuery" | "runMutation" | "runAction"
>;
