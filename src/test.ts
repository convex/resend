/// <reference types="vite/client" />
import type { TestConvex } from "convex-test";
import type { GenericSchema, SchemaDefinition } from "convex/server";
import rateLimiter from "@convex-dev/rate-limiter/test";
import workpool from "@convex-dev/workpool/test";
import type { ComponentApi } from "./component/_generated/component.js";
import schema from "./component/schema.js";
const modules = import.meta.glob("./component/**/*.ts");

/**
 * Register the component and its children.
 * @param t - The test convex instance, e.g. from calling `convexTest`.
 * @param name - The name of the component, as registered in convex.config.ts.
 * @returns a component api to test via ctx.runMutation or for thick client
 *   usage. Also provides types for convex-test's defineTestApp.
 */
export function register(
  t: TestConvex<SchemaDefinition<GenericSchema, boolean>>,
  name: string = "resend",
) {
  rateLimiter.register(t, `${name}/rateLimiter`);
  workpool.register(t, `${name}/emailWorkpool`);
  workpool.register(t, `${name}/callbackWorkpool`);
  return t.registerComponent(name, schema, modules) as unknown as ComponentApi;
}
export default { register, schema, modules };
