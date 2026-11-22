import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { defineSchema, defineTable } from "convex/server";
import { defineTestApp } from "convex-test";
import {
  ManualEmailRecordingError,
  Resend,
  vOnEmailEventArgs,
  type EmailEvent,
} from "./index.js";
import resendTest from "../test.js";

const app = defineTestApp({
  schema: defineSchema({
    events: defineTable(vOnEmailEventArgs),
  }),
  components: {
    resend: resendTest,
    customResend: resendTest,
  },
});
const { createTest, internal } = app.defineModules({
  callbacks: {
    onEmailEvent: app.internalMutation({
      args: vOnEmailEventArgs,
      handler: async (ctx, args) => {
        await ctx.db.insert("events", args);
      },
    }),
  },
});

describe("Resend test registration", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  test.each(["resend", "customResend"] as const)(
    "sends queued emails and runs callbacks with component name %s",
    async (name) => {
      const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
        new Response(JSON.stringify({ data: [{ id: "test-resend-id" }] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
      vi.stubGlobal("fetch", fetchMock);

      const t = createTest();
      const component = app.components[name];
      const resend = new Resend(component, {
        apiKey: "test-api-key",
        onEmailEvent: internal.callbacks.onEmailEvent,
      });
      const emailId = await t.run((ctx) =>
        resend.sendEmail(ctx, {
          from: "sender@example.com",
          to: "delivered@resend.dev",
          subject: "Test email",
          text: "Hello from convex-test",
        }),
      );
      expect(await t.run((ctx) => resend.status(ctx, emailId))).toMatchObject({
        status: "waiting",
      });

      await t.finishAllScheduledFunctions(vi.runAllTimers);

      expect(await t.run((ctx) => resend.get(ctx, emailId))).toMatchObject({
        status: "sent",
        resendId: "test-resend-id",
      });
      expect(fetchMock).toHaveBeenCalledExactlyOnceWith(
        "https://api.resend.com/emails/batch",
        expect.objectContaining({ method: "POST" }),
      );

      const event: EmailEvent = {
        type: "email.delivered",
        created_at: new Date().toISOString(),
        data: {
          created_at: new Date().toISOString(),
          email_id: "test-resend-id",
          from: "sender@example.com",
          to: ["delivered@resend.dev"],
          subject: "Test email",
        },
      };
      await t.mutation(component.lib.handleEmailEvent, { event });
      await t.finishAllScheduledFunctions(vi.runAllTimers);

      expect(await t.run((ctx) => resend.status(ctx, emailId))).toMatchObject({
        status: "delivered",
      });
      const events = await t.run((ctx) => ctx.db.query("events").collect());
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({ id: emailId, event });
    },
  );
});

test("distinguishes a manual send accepted by Resend from a failed tracking write", async () => {
  const t = createTest();
  const resend = new Resend(app.components.resend, { apiKey: "test-api-key" });
  const message = {
    from: "sender@example.com",
    to: "recipient@example.com",
    subject: "First email",
  };
  const resendId = "provider-message-id";
  const recorded = await t.run((ctx) =>
    resend.sendEmailManually(ctx, message, async () => resendId),
  );
  const sendCallback = vi.fn(async () => resendId);
  await t.run(async (ctx) => {
    const error = await resend
      .sendEmailManually(
        ctx,
        { ...message, subject: "Different email" },
        sendCallback,
      )
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ManualEmailRecordingError);
    if (!(error instanceof ManualEmailRecordingError))
      throw new Error("Expected a recording error");
    expect(error.resendId).toBe(resendId);
    expect(error.cause).toMatchObject({
      message: expect.stringContaining("already assigned"),
    });
    expect(await resend.get(ctx, error.emailId)).toMatchObject({
      status: "queued",
    });
    expect(await resend.get(ctx, recorded)).toMatchObject({
      status: "sent",
      resendId,
    });
  });
  expect(sendCallback).toHaveBeenCalledTimes(1);
});
