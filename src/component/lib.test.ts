import { expect, describe, it, beforeEach, afterEach, vi } from "vitest";
import rateLimiter from "@convex-dev/rate-limiter/test";
import workpool from "@convex-dev/workpool/test";
import { api, internal } from "./_generated/api.js";
import { vEmailEvent, type EmailEvent, type RuntimeConfig } from "./shared.js";
import { attemptToParse } from "./utils.js";
import {
  createTestEventOfType,
  createTestRuntimeConfig,
  insertTestEmail,
  insertTestSentEmail,
  setupTest,
  setupTestLastOptions,
  type Tester,
} from "./setup.test.js";
import { type Doc, type Id } from "./_generated/dataModel.js";

describe("handleEmailEvent", () => {
  let t: Tester;
  let event: EmailEvent;
  let email: Doc<"emails">;

  beforeEach(async () => {
    t = setupTest();
    event = createTestEventOfType("email.delivered");
    await setupTestLastOptions(t);
    email = await insertTestSentEmail(t);
  });

  const exec = async (_event: EmailEvent | unknown = event) => {
    await t.mutation(api.lib.handleEmailEvent, { event: _event });
  };

  const getEmail = () =>
    t.run(async (ctx) => {
      const _email = await ctx.db.get("emails", email._id);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

  it("updates email for delivered event", async () => {
    expect(email.status).toBe("sent");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("delivered");
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.finalizedAt).toBeGreaterThan(Date.now() - 10000); // Within last 10 seconds
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) =>
          q.eq("emailId", email._id).eq("eventType", "email.delivered"),
        )
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("email.delivered");
    expect(events[0].resendId).toBe("test-resend-id-123");
  });

  it("updates email for complained event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.complained");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.complained).toBe(true);
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) => q.eq("emailId", email._id))
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("email.complained");
  });

  it("updates email for bounced event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.bounced");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("bounced");
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.finalizedAt).toBeGreaterThan(Date.now() - 10000); // Within last 10 seconds
    expect(updatedEmail.errorMessage).toBe(
      "The email bounced due to invalid recipient",
    );
    // deliveryEvents entry created
    const events = await t.run(async (ctx) =>
      ctx.db
        .query("deliveryEvents")
        .withIndex("by_emailId_eventType", (q) =>
          q.eq("emailId", email._id).eq("eventType", "email.bounced"),
        )
        .collect(),
    );
    expect(events.length).toBe(1);
    expect(events[0].eventType).toBe("email.bounced");
    expect(events[0].message).toBe(
      "The email bounced due to invalid recipient",
    );
  });

  it("updates email for delivery_delayed event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.delivery_delayed");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("delivery_delayed");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
  });

  it("updates email for opened event", async () => {
    expect(email.status).toBe("sent");
    expect(email.opened).toBe(false);
    event = createTestEventOfType("email.opened");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.opened).toBe(true);
  });

  it("does not update email for sent event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.sent");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("updates email for clicked event", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.clicked");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER); // Should remain unchanged
    expect(updatedEmail.clicked).toBe(true); // Now tracks clicks
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("updates email for failed event and changes status", async () => {
    expect(email.status).toBe("sent");
    event = createTestEventOfType("email.failed");

    await exec();

    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("failed"); // Status changes (failed has higher priority than sent)
    expect(updatedEmail.failed).toBe(true); // Flag is set
    expect(updatedEmail.finalizedAt).toBeLessThan(Number.MAX_SAFE_INTEGER); // Should be finalized
    expect(updatedEmail.complained).toBe(false); // Should remain unchanged
    expect(updatedEmail.opened).toBe(false); // Should remain unchanged
  });

  it("gracefully handles invalid event structure - missing type", async () => {
    const invalidEvent = {
      created_at: "2024-01-01T00:00:00Z",
      data: {
        email_id: "test-resend-id-123",
        from: "test@example.com",
        to: "recipient@example.com",
        subject: "Test Email",
      },
    };

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles invalid event structure - missing data", async () => {
    const invalidEvent = {
      type: "email.delivered",
      created_at: "2024-01-01T00:00:00Z",
    };

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles completely invalid event", async () => {
    const invalidEvent = "not an object";

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles null event", async () => {
    // Should not throw an error
    await exec(null);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });

  it("gracefully handles empty object event", async () => {
    const invalidEvent = {};

    // Should not throw an error
    await exec(invalidEvent);

    // Email should remain unchanged
    const updatedEmail = await getEmail();
    expect(updatedEmail.status).toBe("sent");
    expect(updatedEmail.finalizedAt).toBe(Number.MAX_SAFE_INTEGER);
    expect(updatedEmail.complained).toBe(false);
    expect(updatedEmail.opened).toBe(false);
  });
});

describe("vEmailEvent parsing", () => {
  it("keeps message_id but strips unknown fields", () => {
    const sent = createTestEventOfType("email.sent");
    const rawWebhook = {
      ...sent,
      data: { ...sent.data, some_future_resend_field: "should be stripped" },
    };

    const result = attemptToParse(vEmailEvent, rawWebhook as unknown);
    if (result.kind !== "success") throw new Error(String(result.error));

    expect(result.data.data.message_id).toBe("<test-message-id@example.com>");
    expect("some_future_resend_field" in result.data.data).toBe(false);
  });
});

describe("sendEmail with templates", () => {
  let t: Tester;

  beforeEach(async () => {
    t = setupTest();
    await setupTestLastOptions(t);
  });

  it("should accept template-based email", async () => {
    const emailId: Id<"emails"> = await t.mutation(api.lib.sendEmail, {
      options: {
        apiKey: "test-key",
        initialBackoffMs: 1000,
        retryAttempts: 3,
        testMode: true,
      },
      from: "test@resend.dev",
      to: ["delivered@resend.dev"],
      template: {
        id: "order-confirmation",
        variables: {
          PRODUCT: "Vintage Macintosh",
          PRICE: 499,
        },
      },
    });

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get("emails", emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.template?.id).toBe("order-confirmation");
    expect(email.template?.variables).toEqual({
      PRODUCT: "Vintage Macintosh",
      PRICE: 499,
    });
    expect(email.subject).toBeUndefined();
    expect(email.html).toBeUndefined();
    expect(email.text).toBeUndefined();
    expect(email.status).toBe("waiting");
  });

  it("should reject email with both template and html/text", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          apiKey: "test-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@resend.dev",
        to: ["delivered@resend.dev"],
        subject: "Test",
        html: "<p>Test</p>",
        template: {
          id: "order-confirmation",
          variables: {
            PRODUCT: "Test",
          },
        },
      }),
    ).rejects.toThrow("Cannot provide both html/text and template");
  });

  it("should accept template email with optional subject", async () => {
    const emailId: Id<"emails"> = await t.mutation(api.lib.sendEmail, {
      options: {
        apiKey: "test-key",
        initialBackoffMs: 1000,
        retryAttempts: 3,
        testMode: true,
      },
      from: "test@resend.dev",
      to: ["delivered@resend.dev"],
      subject: "Custom Subject Override",
      template: {
        id: "order-confirmation",
        variables: {
          PRODUCT: "Test",
        },
      },
    });

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get("emails", emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.template?.id).toBe("order-confirmation");
    expect(email.template?.variables).toEqual({
      PRODUCT: "Test",
    });
    expect(email.subject).toBe("Custom Subject Override");
    expect(email.status).toBe("waiting");
  });

  it("should reject email without content or template", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          apiKey: "test-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@resend.dev",
        to: ["delivered@resend.dev"],
      }),
    ).rejects.toThrow("Either html/text or template must be provided");
  });

  it("should reject traditional email without subject", async () => {
    await expect(
      t.mutation(api.lib.sendEmail, {
        options: {
          apiKey: "test-key",
          initialBackoffMs: 1000,
          retryAttempts: 3,
          testMode: true,
        },
        from: "test@resend.dev",
        to: ["delivered@resend.dev"],
        html: "<p>Test</p>",
      }),
    ).rejects.toThrow("Subject is required when not using a template");
  });
});

describe("sendEmail with idempotencyKey", () => {
  let t: Tester;

  const baseOptions = {
    apiKey: "test-key",
    initialBackoffMs: 1000,
    retryAttempts: 3,
    testMode: true,
  };

  beforeEach(async () => {
    t = setupTest();
    await setupTestLastOptions(t);
  });

  const send = (idempotencyKey?: string) =>
    t.mutation(api.lib.sendEmail, {
      options: baseOptions,
      from: "test@resend.dev",
      to: ["delivered@resend.dev"],
      subject: "Test",
      html: "<p>Test</p>",
      idempotencyKey,
    });

  it("persists the idempotencyKey on the email", async () => {
    const emailId: Id<"emails"> = await send("trigger-123");

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get("emails", emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.idempotencyKey).toBe("trigger-123");
  });

  it("returns the existing email id on a duplicate enqueue", async () => {
    const first: Id<"emails"> = await send("trigger-123");
    const second: Id<"emails"> = await send("trigger-123");

    expect(second).toBe(first);

    // Only one email should have been inserted.
    const emails = await t.run(async (ctx) => ctx.db.query("emails").collect());
    expect(emails.length).toBe(1);
  });

  it("enqueues separate emails for distinct keys", async () => {
    const first: Id<"emails"> = await send("trigger-a");
    const second: Id<"emails"> = await send("trigger-b");

    expect(second).not.toBe(first);

    const emails = await t.run(async (ctx) => ctx.db.query("emails").collect());
    expect(emails.length).toBe(2);
  });

  it("does not dedupe when no key is provided", async () => {
    const first: Id<"emails"> = await send();
    const second: Id<"emails"> = await send();

    expect(second).not.toBe(first);

    const emails = await t.run(async (ctx) => ctx.db.query("emails").collect());
    expect(emails.length).toBe(2);
  });
});

describe("createManualEmail", () => {
  let t: Tester;

  beforeEach(async () => {
    t = setupTest();
  });

  it("persists cc and bcc recipients", async () => {
    const emailId: Id<"emails"> = await t.mutation(api.lib.createManualEmail, {
      from: "test@resend.dev",
      to: "delivered@resend.dev",
      cc: "cc@resend.dev",
      bcc: ["bcc@resend.dev"],
      subject: "Manual email",
    });

    const email = await t.run(async (ctx) => {
      const _email = await ctx.db.get("emails", emailId);
      if (!_email) throw new Error("Email not found");
      return _email;
    });

    expect(email.to).toEqual(["delivered@resend.dev"]);
    expect(email.cc).toEqual(["cc@resend.dev"]);
    expect(email.bcc).toEqual(["bcc@resend.dev"]);
  });
});

describe("per-email options", () => {
  let t: Tester;

  const appOptions: RuntimeConfig = {
    apiKey: "re_app",
    testMode: false,
    initialBackoffMs: 1000,
    retryAttempts: 3,
    onEmailEvent: { fnHandle: "function://app-callback" },
  };
  const authOptions: RuntimeConfig = {
    apiKey: "re_auth",
    testMode: false,
    initialBackoffMs: 30000,
    retryAttempts: 5,
  };

  beforeEach(() => {
    vi.useFakeTimers();
    t = setupTest();
    rateLimiter.register(t);
    workpool.register(t, "emailWorkpool");
    workpool.register(t, "callbackWorkpool");
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  const send = (options: RuntimeConfig, to: string) =>
    t.mutation(api.lib.sendEmail, {
      options,
      from: "test@example.com",
      to: [to],
      subject: "Test",
      html: "<p>Test</p>",
    });

  const getEmail = (emailId: Id<"emails">) =>
    t.run(async (ctx) => {
      const email = await ctx.db.get("emails", emailId);
      if (!email) throw new Error("Email not found");
      return email;
    });

  // Mock the Resend batch API, returning ids like "<apiKey>-<index>".
  const mockFetch = () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockImplementation(async (_input, init) => {
        const headers = init?.headers as Record<string, string>;
        const apiKey = headers.Authorization.replace("Bearer ", "");
        const body = JSON.parse(init?.body as string) as unknown[];
        return new Response(
          JSON.stringify({
            data: body.map((_, i) => ({ id: `${apiKey}-${i}` })),
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      });
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  };

  // The recipients sent with each API key.
  const sentByApiKey = (fetchMock: ReturnType<typeof mockFetch>) =>
    fetchMock.mock.calls.map(([, init]) => ({
      apiKey: (init?.headers as Record<string, string>).Authorization,
      to: (JSON.parse(init?.body as string) as { to: string[] }[]).map(
        (e) => e.to,
      ),
    }));

  it("stores options per email and reuses identical options", async () => {
    const first = await getEmail(await send(appOptions, "a@example.com"));
    const second = await getEmail(await send(authOptions, "b@example.com"));
    const third = await getEmail(await send(appOptions, "c@example.com"));

    expect(first.optionsId).toBeDefined();
    expect(second.optionsId).toBeDefined();
    expect(second.optionsId).not.toBe(first.optionsId);
    expect(third.optionsId).toBe(first.optionsId);

    const rows = await t.run((ctx) => ctx.db.query("emailOptions").collect());
    expect(rows.map((r) => r.options)).toEqual([appOptions, authOptions]);

    // lastOptions is still maintained for emails without their own options.
    const lastOptions = await t.run((ctx) =>
      ctx.db.query("lastOptions").unique(),
    );
    expect(lastOptions?.options).toEqual(appOptions);
  });

  it("sends each group of a mixed batch with its own api key", async () => {
    const fetchMock = mockFetch();
    const a = await send(appOptions, "a@example.com");
    const b = await send(authOptions, "b@example.com");
    const c = await send(appOptions, "c@example.com");

    await t.finishAllScheduledFunctions(vi.runAllTimers);

    expect(sentByApiKey(fetchMock)).toEqual(
      expect.arrayContaining([
        { apiKey: "Bearer re_app", to: [["a@example.com"], ["c@example.com"]] },
        { apiKey: "Bearer re_auth", to: [["b@example.com"]] },
      ]),
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(await getEmail(a)).toMatchObject({
      status: "sent",
      resendId: "re_app-0",
    });
    expect(await getEmail(b)).toMatchObject({
      status: "sent",
      resendId: "re_auth-0",
    });
    expect(await getEmail(c)).toMatchObject({
      status: "sent",
      resendId: "re_app-1",
    });
  });

  it("falls back to lastOptions for emails without their own options", async () => {
    const fetchMock = mockFetch();
    await setupTestLastOptions(t, {
      options: { ...createTestRuntimeConfig(), apiKey: "re_last" },
    });
    const waiting = {
      from: "test@example.com",
      subject: "Test",
      replyTo: [],
      status: "waiting" as const,
      bounced: false,
      complained: false,
      failed: false,
      deliveryDelayed: false,
      opened: false,
      clicked: false,
      segment: 0,
      finalizedAt: Number.MAX_SAFE_INTEGER,
    };
    // Enqueued before options were stored per email.
    const legacy = await insertTestEmail(t, {
      ...waiting,
      to: ["legacy@example.com"],
    });
    // Points at an options row that no longer exists.
    const missingOptionsId = await t.run(async (ctx) => {
      const id = await ctx.db.insert("emailOptions", { options: appOptions });
      await ctx.db.delete("emailOptions", id);
      return id;
    });
    const missing = await insertTestEmail(t, {
      ...waiting,
      to: ["missing@example.com"],
      optionsId: missingOptionsId,
    });
    await t.run(async (ctx) => {
      const runId = await ctx.scheduler.runAfter(0, internal.lib.makeBatch, {
        reloop: false,
        segment: 10,
      });
      await ctx.db.insert("nextBatchRun", { runId });
    });

    await t.finishAllScheduledFunctions(vi.runAllTimers);

    expect(sentByApiKey(fetchMock)).toEqual(
      expect.arrayContaining([
        { apiKey: "Bearer re_last", to: [["legacy@example.com"]] },
        { apiKey: "Bearer re_last", to: [["missing@example.com"]] },
      ]),
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect((await getEmail(legacy._id)).status).toBe("sent");
    expect((await getEmail(missing._id)).status).toBe("sent");
  });
});
