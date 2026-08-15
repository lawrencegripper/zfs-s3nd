import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./test/ui",
  testMatch: "**/*.spec.ts",
  fullyParallel: false,
  workers: 1,
  // The journey intentionally shares server state across serial tests, so an
  // individual retry would inherit a partially completed setup.
  retries: 0,
  reporter: process.env.CI ? [["github"], ["html", { open: "never" }]] : "list",
  use: {
    baseURL: "http://127.0.0.1:3100",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    permissions: ["clipboard-read", "clipboard-write"],
  },
  webServer: {
    command: "go run ./test/ui/fixture",
    url: "http://127.0.0.1:3100/healthz",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      ...process.env,
      UI_TEST_DATABASE_PATH: "./.ui-test/catalog.db",
    },
  },
});
