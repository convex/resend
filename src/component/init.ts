import { v } from "convex/values";
import { internalMutation } from "./_generated/server.js";
import { DEFAULT_CONFIG, vInitArgs } from "./shared.js";

export const init = internalMutation({
  args: vInitArgs,
  returns: v.null(),
  handler: async (ctx, args) => {
    const config = {
      testMode: args.testMode ?? DEFAULT_CONFIG.testMode,
      initialBackoffMs:
        args.initialBackoffMs ?? DEFAULT_CONFIG.initialBackoffMs,
      retryAttempts: args.retryAttempts ?? DEFAULT_CONFIG.retryAttempts,
      onEmailEvent: args.onEmailEvent,
    };
    const existing = await ctx.db.query("config").unique();
    if (existing) {
      await ctx.db.replace("config", existing._id, config);
    } else {
      await ctx.db.insert("config", config);
    }
    return null;
  },
});
