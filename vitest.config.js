import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "edge-runtime",
    // convex-test loads each component's convex.config, and published
    // batch-worker configs import "./logging" without an extension.
    server: { deps: { inline: [/@convex-dev\//] } },
    typecheck: {
      tsconfig: "./tsconfig.test.json",
    },
  },
});
