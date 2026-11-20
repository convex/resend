/// <reference types="vite/client" />
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { convexTest } from "convex-test";
import { components } from "./_generated/api.js";
import exampleSchema from "./schema.js";
import resendTest from "../../src/test.js";
import type { EmailEvent } from "../../src/client/index.js";

const exampleModules = import.meta.glob("./**/*.*s");

function setup(componentArgs?: Record<string, Record<string, unknown>>) {
  const t = convexTest({
    schema: exampleSchema,
    modules: exampleModules,
    componentArgs,
  });
  resendTest.register(t);
  return t;
}

// `runInComponent` isn't in convex-test's public types.
function inResend<T>(
  t: ReturnType<typeof setup>,
  fn: (ctx: any) => Promise<T>,
): Promise<T> {
  return (t as any).runInComponent("resend", fn);
}

function readConfig(t: ReturnType<typeof setup>) {
  return inResend<any>(t, (ctx) => ctx.db.query("config").unique());
}

// What the `Resend` client passes on each call, which init args override.
function clientOptions(overrides?: { testMode?: boolean }) {
  return {
    apiKey: "test-api-key",
    initialBackoffMs: 30000,
    retryAttempts: 5,
    testMode: true,
    ...overrides,
  };
}

describe("resend init", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.stubEnv("RESEND_API_KEY", "test-api-key");
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  });

  test("stores the args from the example's convex.config", async () => {
    const t = setup();
    expect(await readConfig(t)).toMatchObject({
      testMode: true,
      onEmailEvent: "function://;example:handleEmailEvent",
    });
  });

  test("leaves the config document alone on a push with the same args", async () => {
    const t = setup();
    const before = await readConfig(t);
    await t.runInits();
    const configs = await inResend(t, (ctx) =>
      ctx.db.query("config").collect(),
    );
    expect(configs).toEqual([before]);
  });

  test("calls the app's onEmailEvent handle on a webhook event", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof fetch>().mockResolvedValue(
        new Response(JSON.stringify({ data: [{ id: "resend-id-1" }] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const t = setup();
    const emailId = await t.mutation(components.resend.lib.sendEmail, {
      options: clientOptions(),
      from: "sender@example.com",
      to: ["delivered@resend.dev"],
      subject: "Hi",
      text: "Hello",
    });
    await t.run((ctx) =>
      ctx.db.insert("testEmails", {
        email: emailId,
        expectation: "delivered",
      }),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);

    const event: EmailEvent = {
      type: "email.delivered",
      created_at: new Date().toISOString(),
      data: {
        created_at: new Date().toISOString(),
        email_id: "resend-id-1",
        from: "sender@example.com",
        to: ["delivered@resend.dev"],
        subject: "Hi",
      },
    };
    await t.mutation(components.resend.lib.handleEmailEvent, { event });
    await t.finishAllScheduledFunctions(vi.runAllTimers);

    // example.handleEmailEvent deletes a delivered email's expectation.
    expect(await t.run((ctx) => ctx.db.query("testEmails").collect())).toEqual(
      [],
    );
  });

  test("testMode from the init args wins over the client's option", async () => {
    const t = setup();
    await expect(
      t.mutation(components.resend.lib.sendEmail, {
        options: clientOptions({ testMode: false }),
        from: "sender@example.com",
        to: ["someone@example.com"],
        subject: "Hi",
        text: "Hello",
      }),
    ).rejects.toThrow(/Test mode is enabled/);
  });

  test("componentArgs override the convex.config args", async () => {
    const t = setup({ resend: { testMode: false } });
    expect(await readConfig(t)).toEqual(
      expect.objectContaining({ testMode: false }),
    );
    expect((await readConfig(t))?.onEmailEvent).toBeUndefined();
    await t.mutation(components.resend.lib.sendEmail, {
      options: clientOptions({ testMode: true }),
      from: "sender@example.com",
      to: ["someone@example.com"],
      subject: "Hi",
      text: "Hello",
    });
  });
});
