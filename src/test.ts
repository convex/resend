/// <reference types="vite/client" />
import type { TestConvex } from "convex-test";
import type {
  ComponentArgInput,
  FunctionArgs,
  GenericSchema,
  SchemaDefinition,
} from "convex/server";
import rateLimiter from "@convex-dev/rate-limiter/test";
import workpool from "@convex-dev/workpool/test";
import type { ComponentApi } from "./component/_generated/component.js";
import type { internal } from "./component/_generated/api.js";
import schema from "./component/schema.js";

type InitArgs = FunctionArgs<typeof internal.init.init>;
export type ResendTestArgs = {
  [K in keyof InitArgs]?: ComponentArgInput<InitArgs[K]>;
};
const modules = import.meta.glob("./component/**/*.ts");

/**
 * Register the component and its children.
 * @param t - The test convex instance, e.g. from calling `convexTest`.
 * @param name - The name of the component, as registered in convex.config.ts.
 * @param args - Arguments for the component's init, in place of the ones the
 *   app's convex.config.ts passes in `app.use(resend, { args })`.
 * @returns a component api to test via ctx.runMutation or for thick client
 *   usage. Also provides types for convex-test's defineTestApp.
 */
export function register(
  t: TestConvex<SchemaDefinition<GenericSchema, boolean>>,
  name: string = "resend",
  args?: ResendTestArgs,
) {
  rateLimiter.register(t, `${name}/rateLimiter`);
  workpool.register(t, `${name}/emailWorkpool`);
  workpool.register(t, `${name}/callbackWorkpool`);
  return t.registerComponent(name, schema, modules, {
    args,
  }) as unknown as ComponentApi;
}
export default { register, schema, modules };
