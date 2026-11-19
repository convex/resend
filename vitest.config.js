import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "convex-test": fileURLToPath(
        import.meta.resolve("./vendor/convex-test-init/index.ts"),
      ),
    },
  },
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
