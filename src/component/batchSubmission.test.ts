import type { WorkId } from "@convex-dev/workpool";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api, internal } from "./_generated/api.js";
import {
  createTestEventOfType,
  insertTestSentEmail,
  setupTest,
} from "./setup.test.js";

afterEach(() => vi.restoreAllMocks());

describe("batch submission", () => {
  it("preserves delivery history when an accepted batch loses its acknowledgement", async () => {
    const t = setupTest();
    const a = await insertTestSentEmail(t);
    const b = await insertTestSentEmail(t);
    await t.run(async (ctx) => {
      for (const row of [a, b]) {
        await ctx.db.patch("emails", row._id, {
          status: "queued",
          resendId: undefined,
          batchSubmission: "pending",
        });
      }
    });
    const request = { apiKey: "re_fictional", emails: [a._id, b._id] };
    const send = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("Lost acknowledgement"))
      .mockImplementation(
        async () => new Response("Forbidden", { status: 403 }),
      );

    await expect(
      t.action(internal.lib.callResendAPIWithBatch, request),
    ).rejects.toThrow("Lost acknowledgement");
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, request),
    ).rejects.toThrow("HTTP 403");
    const firstBody = send.mock.calls[0][1]?.body;
    if (typeof firstBody !== "string")
      throw new Error("Expected the batch body");
    expect(JSON.parse(firstBody)).toMatchObject([
      { tags: [{ name: "convex_email_id", value: a._id }] },
      { tags: [{ name: "convex_email_id", value: b._id }] },
    ]);

    const bounce = createTestEventOfType("email.bounced");
    await t.mutation(api.lib.handleEmailEvent, {
      event: {
        ...bounce,
        data: {
          ...bounce.data,
          email_id: "provider-a",
          tags: { convex_email_id: a._id },
        },
      },
    });
    await expect(
      t.mutation(api.lib.cancelEmail, { emailId: b._id }),
    ).rejects.toThrow("already been sent");
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, request),
    ).rejects.toThrow("HTTP 403");
    expect(send.mock.calls).toHaveLength(3);
    expect(send.mock.calls[1]).toEqual(send.mock.calls[0]);
    expect(send.mock.calls[2]).toEqual(send.mock.calls[0]);

    await t.mutation(internal.lib.onEmailComplete, {
      workId: "test-work" as WorkId,
      context: { emailIds: request.emails },
      result: { kind: "failed", error: "No acknowledgement after retries" },
    });
    expect(await t.query(api.lib.getStatus, { emailId: b._id })).toMatchObject({
      status: "queued",
    });
    const delivered = createTestEventOfType("email.delivered");
    await t.mutation(api.lib.handleEmailEvent, {
      event: {
        ...delivered,
        data: {
          ...delivered.data,
          email_id: "provider-b",
          tags: { convex_email_id: b._id },
        },
      },
    });
    expect(await t.query(api.lib.getStatus, { emailId: a._id })).toMatchObject({
      status: "bounced",
    });
    expect(await t.query(api.lib.getStatus, { emailId: b._id })).toMatchObject({
      status: "delivered",
    });
  });

  it("marks a new batch rejected on its known first attempt", async () => {
    const t = setupTest();
    const email = await insertTestSentEmail(t);
    await t.run((ctx) =>
      ctx.db.patch("emails", email._id, {
        status: "queued",
        resendId: undefined,
        batchSubmission: "pending",
      }),
    );
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("Forbidden", { status: 403 }),
    );
    await t.action(internal.lib.callResendAPIWithBatch, {
      apiKey: "re_fictional",
      emails: [email._id],
    });
    expect(
      await t.query(api.lib.getStatus, { emailId: email._id }),
    ).toMatchObject({ status: "failed" });
  });

  it("preserves the provider payload for emails queued before the upgrade", async () => {
    const t = setupTest();
    const email = await insertTestSentEmail(t);
    await t.run((ctx) =>
      ctx.db.patch("emails", email._id, {
        status: "queued",
        resendId: undefined,
      }),
    );
    const send = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(new Response("Forbidden", { status: 403 }));
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, {
        apiKey: "re_fictional",
        emails: [email._id],
      }),
    ).rejects.toThrow("HTTP 403");
    const body = send.mock.calls[0][1]?.body;
    if (typeof body !== "string") throw new Error("Expected the batch body");
    expect(JSON.parse(body)[0]).not.toHaveProperty("tags");
    expect(
      await t.query(api.lib.getStatus, { emailId: email._id }),
    ).toMatchObject({ status: "queued" });
  });
});
