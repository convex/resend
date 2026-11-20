import { defineApp } from "convex/server";
import { v } from "convex/values";
import resend from "@convex-dev/resend/convex.config";
import { internal } from "./_generated/api";

const app = defineApp({
  env: {
    RESEND_API_KEY: v.string(),
  },
});
app.use(resend, {
  args: {
    testMode: true,
    onEmailEvent: internal.example.handleEmailEvent,
  },
  env: { RESEND_API_KEY: app.env.RESEND_API_KEY },
});

export default app;
