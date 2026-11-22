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
    ).rejects.toThrow("Resend API error: Forbidden");
    const firstBody = send.mock.calls[0][1]?.body;
    if (typeof firstBody !== "string")
      throw new Error("Expected the batch body");
    const [first, second] = JSON.parse(firstBody);
    const aReference: string = first.tags[0].value;
    const bReference: string = second.tags[0].value;
    expect(aReference.startsWith(a._id + "_")).toBe(true);
    expect(bReference.startsWith(b._id + "_")).toBe(true);

    send.mockResolvedValueOnce(
      Response.json({ data: [{ id: "provider-a" }, { id: "provider-a" }] }),
    );
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, request),
    ).rejects.toThrow("duplicate message IDs");

    const bounce = createTestEventOfType("email.bounced");
    await t.mutation(api.lib.handleEmailEvent, {
      event: {
        ...bounce,
        data: {
          ...bounce.data,
          email_id: "provider-a",
          tags: { convex_email_id: aReference },
        },
      },
    });
    await expect(
      t.mutation(api.lib.cancelEmail, { emailId: b._id }),
    ).rejects.toThrow("already been sent");
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, request),
    ).rejects.toThrow("Resend API error: Forbidden");
    expect(send.mock.calls).toHaveLength(4);
    for (const call of send.mock.calls)
      expect(call).toEqual(send.mock.calls[0]);

    await t.mutation(internal.lib.onEmailComplete, {
      workId: "test-work" as WorkId,
      context: { emailIds: request.emails },
      result: { kind: "failed", error: "No acknowledgement after retries" },
    });
    expect(await t.query(api.lib.getStatus, { emailId: b._id })).toMatchObject({
      status: "failed",
      errorMessage: "Resend submission was not confirmed",
    });
    await t.mutation(internal.lib.onEmailComplete, {
      workId: "test-work" as WorkId,
      context: { emailIds: request.emails },
      result: {
        kind: "success",
        returnValue: {
          emailIds: request.emails,
          resendIds: ["provider-a", "provider-a"],
        },
      },
    });
    expect(await t.query(api.lib.get, { emailId: b._id })).not.toHaveProperty(
      "resendId",
    );
    const delivered = createTestEventOfType("email.delivered");
    await t.mutation(api.lib.handleEmailEvent, {
      event: {
        ...delivered,
        data: {
          ...delivered.data,
          email_id: "provider-b",
          tags: { convex_email_id: bReference },
        },
      },
    });
    expect(await t.query(api.lib.getStatus, { emailId: a._id })).toMatchObject({
      status: "bounced",
    });
    expect(await t.query(api.lib.getStatus, { emailId: b._id })).toMatchObject({
      status: "delivered",
    });
    expect(await t.query(api.lib.get, { emailId: b._id })).not.toHaveProperty(
      "batchSubmission",
    );
  });

  it("does not associate a copied batch reference with a different manual email", async () => {
    const t = setupTest();
    const email = await insertTestSentEmail(t, {
      status: "queued",
      resendId: undefined,
      batchSubmission: "pending",
    });
    const send = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValue(new Error("No acknowledgement"));
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, {
        apiKey: "re_fictional",
        emails: [email._id],
      }),
    ).rejects.toThrow("No acknowledgement");
    const payload = send.mock.calls[0][1]?.body;
    if (typeof payload !== "string")
      throw new Error("Expected a batch payload");
    const manual = await t.mutation(api.lib.createManualEmail, {
      from: "from@example.com",
      to: "to@example.com",
      subject: "Manual",
    });
    const delivered = createTestEventOfType("email.delivered");
    const event = {
      ...delivered,
      data: {
        ...delivered.data,
        email_id: "provider-manual",
        from: "from@example.com",
        to: ["to@example.com"],
        subject: "Manual",
        tags: JSON.parse(payload)[0].tags,
      },
    };
    await t.mutation(api.lib.handleEmailEvent, { event });
    expect(
      await t.query(api.lib.get, { emailId: email._id }),
    ).not.toHaveProperty("resendId");
    await t.mutation(api.lib.updateManualEmail, {
      emailId: manual,
      status: "sent",
      resendId: event.data.email_id,
    });
    await t.mutation(api.lib.handleEmailEvent, { event });
    expect(await t.query(api.lib.getStatus, { emailId: manual })).toMatchObject(
      { status: "delivered" },
    );
  });

  it("rejects a pending completion with duplicate IDs without binding either row", async () => {
    const t = setupTest();
    const a = await insertTestSentEmail(t, {
      status: "queued",
      resendId: undefined,
    });
    const b = await insertTestSentEmail(t, {
      status: "queued",
      resendId: undefined,
    });
    const emailIds = [a._id, b._id];
    await t.mutation(internal.lib.onEmailComplete, {
      workId: "old-work" as WorkId,
      context: { emailIds },
      result: {
        kind: "success",
        returnValue: { emailIds, resendIds: ["duplicate", "duplicate"] },
      },
    });
    for (const emailId of emailIds) {
      expect(await t.query(api.lib.get, { emailId })).not.toHaveProperty(
        "resendId",
      );
      expect(await t.query(api.lib.getStatus, { emailId })).toMatchObject({
        status: "failed",
      });
    }
  });

  it("preserves manual delivery events with an app-owned reference tag", async () => {
    const t = setupTest();
    const original = await t.mutation(api.lib.createManualEmail, {
      from: "from@example.com",
      to: "to@example.com",
      subject: "Manual email",
    });
    const providerId = "provider-manual";
    await t.mutation(api.lib.updateManualEmail, {
      emailId: original,
      status: "sent",
      resendId: providerId,
    });
    const delivered = createTestEventOfType("email.delivered");
    await t.mutation(api.lib.handleEmailEvent, {
      event: {
        ...delivered,
        data: {
          ...delivered.data,
          email_id: providerId,
          tags: { convex_email_id: "app_value" },
        },
      },
    });
    expect(
      await t.query(api.lib.getStatus, { emailId: original }),
    ).toMatchObject({ status: "delivered" });
    const manualId = await t.mutation(api.lib.createManualEmail, {
      from: "from@example.com",
      to: "to@example.com",
      subject: "Manual email",
    });
    await expect(
      t.mutation(api.lib.updateManualEmail, {
        emailId: manualId,
        status: "sent",
        resendId: providerId,
      }),
    ).rejects.toThrow("already assigned");
  });

  it("submits surviving emails when a cancelled row was cleaned before the first attempt", async () => {
    const t = setupTest();
    const cancelled = await insertTestSentEmail(t);
    const remaining = await insertTestSentEmail(t);
    await t.run(async (ctx) => {
      await ctx.db.delete("emails", cancelled._id);
      await ctx.db.patch("emails", remaining._id, {
        status: "queued",
        resendId: undefined,
        batchSubmission: "pending",
      });
    });
    const send = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValue(
        Response.json({ data: [{ id: "provider-remaining" }] }),
      );
    await expect(
      t.action(internal.lib.callResendAPIWithBatch, {
        apiKey: "re_fictional",
        emails: [cancelled._id, remaining._id],
      }),
    ).resolves.toEqual({
      emailIds: [remaining._id],
      resendIds: ["provider-remaining"],
    });
    expect(send).toHaveBeenCalledTimes(1);
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
    ).rejects.toThrow("Resend API error: Forbidden");
    const body = send.mock.calls[0][1]?.body;
    if (typeof body !== "string") throw new Error("Expected the batch body");
    expect(JSON.parse(body)[0]).not.toHaveProperty("tags");
    expect(
      await t.query(api.lib.getStatus, { emailId: email._id }),
    ).toMatchObject({ status: "queued" });
  });
});
