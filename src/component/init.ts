import { v } from "convex/values";
import { internalMutation } from "./_generated/server.js";
import { vInitArgs } from "./shared.js";

/**
 * Runs in every push's transaction. Writes only when the args changed, since
 * sendEmail and makeBatch read the config document and a write on each push
 * would conflict with them.
 */
export const init = internalMutation({
  args: vInitArgs.fields,
  returns: v.null(),
  handler: async (ctx, args) => {
    const existing = await ctx.db.query("config").unique();
    if (!existing) {
      await ctx.db.insert("config", args);
    } else if (
      existing.testMode !== args.testMode ||
      existing.initialBackoffMs !== args.initialBackoffMs ||
      existing.retryAttempts !== args.retryAttempts ||
      existing.onEmailEvent !== args.onEmailEvent
    ) {
      await ctx.db.replace("config", existing._id, args);
    }
    return null;
  },
});
