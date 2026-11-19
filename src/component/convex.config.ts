import { defineComponent } from "convex/server";
import { v } from "convex/values";
import rateLimiter from "@convex-dev/rate-limiter/convex.config";
import workpool from "@convex-dev/workpool/convex.config";
import { internal } from "./_generated/api.js";

const component = defineComponent("resend", {
  init: internal.init.init,
  env: {
    RESEND_API_KEY: v.optional(v.string()),
  },
});
component.use(rateLimiter);
component.use(workpool, { name: "emailWorkpool" });
component.use(workpool, { name: "callbackWorkpool" });

export default component;
