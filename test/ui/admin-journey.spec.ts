import { expect, test } from "@playwright/test";

const adminPassword = "correct horse battery staple";
const publicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBusZfErqqoVpdl2I6SMezxQQKDZrvD7IJXYDLdjWIwJ ui-test";

async function signIn(page: import("@playwright/test").Page) {
  await page.goto("/");
  if (await page.getByRole("heading", { level: 1, name: "Admin sign in" }).isVisible()) {
    await page.getByLabel("Admin password", { exact: true }).fill(adminPassword);
    await page.getByRole("button", { name: "Sign in" }).click();
  }
  await expect(page.getByRole("heading", { level: 1, name: "Dashboard" })).toBeVisible();
}

test.describe.serial("single-admin journey", () => {
  test("guides first-time setup into the backup workflow", async ({ page }) => {
    await page.goto("/");

    await expect(page).toHaveURL(/\/setup$/);
    await expect(page.getByRole("heading", { level: 1, name: "Secure this backup target" })).toBeVisible();
    await expect(page.getByText("single administrator password", { exact: false })).toBeVisible();
    await expect(page.getByLabel("Admin password", { exact: true })).toHaveAttribute("autocomplete", "new-password");
    await expect(page.getByLabel("Confirm admin password")).toHaveAttribute("autocomplete", "new-password");

    await page.getByLabel("Admin password", { exact: true }).fill(adminPassword);
    await page.getByLabel("Confirm admin password").fill(adminPassword);
    await page.getByRole("button", { name: "Save password and continue" }).click();

    await expect(page).toHaveURL(/\/(?:\?.*)?$/);
    await expect(page.getByRole("heading", { level: 1, name: "Dashboard" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Back up your first dataset" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "1. Authorize an SSH key" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "2. Send a snapshot" })).toBeVisible();
    await expect(page.getByText("Keep the private key on the source system", { exact: false })).toBeVisible();

    const setup = page.getByRole("region", { name: "Back up your first dataset" });
    await setup.getByRole("button", { name: "Copy command" }).click();
    await expect(setup.getByRole("button", { name: "Copy command" }).getByText("Copied")).toBeVisible();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toContain("zfs send tank/photos@zs3-first");
  });

  test("supports sign out and an explicit admin sign in", async ({ page }) => {
    await signIn(page);
    await page.getByRole("button", { name: "Sign out" }).click();

    await expect(page.getByRole("heading", { level: 1, name: "Admin sign in" })).toBeVisible();
    await expect(page.getByLabel("Admin password", { exact: true })).toHaveAttribute("autocomplete", "current-password");
    await page.getByLabel("Admin password", { exact: true }).fill(adminPassword);
    await page.getByRole("button", { name: "Sign in" }).click();
    await expect(page).toHaveURL(/\/(?:\?.*)?$/);
  });

  test("explains empty operational views without dead ends", async ({ page }) => {
    await signIn(page);

    await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Datasets" }).click();
    await expect(page.getByRole("heading", { level: 1, name: "Datasets" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "No datasets yet" })).toBeVisible();

    await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Status" }).click();
    await expect(page.getByRole("heading", { level: 1, name: "Status" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Checks" })).toBeVisible();
    await expect(page.getByText("Catalog", { exact: true })).toBeVisible();
    await expect(page.getByText("Storage", { exact: true })).toBeVisible();
  });

  test("keeps primary administration pages easy to reach", async ({ page, request }) => {
    await signIn(page);
    const navigation = page.getByRole("navigation", { name: "Primary" });
    for (const name of ["Dashboard", "Datasets", "Activity", "Status", "Settings"]) {
      await expect(navigation.getByRole("link", { name })).toBeVisible();
    }

    await navigation.getByRole("link", { name: "Settings" }).click();
    await expect(page.getByRole("heading", { level: 1, name: "Access and integrations" })).toBeVisible();
    await expect(page.getByText("Public keys only", { exact: false })).toBeVisible();

    await page.getByLabel("Key name").fill("nas-home");
    await page.getByLabel("Public key").fill(publicKey);
    await page.getByRole("button", { name: "Add public key" }).click();
    await expect(page.getByText("SSH key added", { exact: false })).toBeVisible();
    await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Settings" }).click();
    await expect(page.getByRole("cell", { name: "nas-home" })).toBeVisible();

    const seed = await request.post("/__fixture/seed-dataset");
    expect(seed.ok()).toBeTruthy();
    await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Datasets" }).click();
    const mainWidth = await page.locator("main.datasets-main").evaluate((element) => element.getBoundingClientRect().width);
    expect(mainWidth).toBeGreaterThan(1200);
    const dataset = page.locator("tr.dataset-row").filter({ hasText: "photos" });
    await expect(dataset.getByRole("link", { name: "photos" })).toBeVisible();
    const viewButton = dataset.getByRole("link", { name: "View" });
    const validateButton = dataset.getByRole("button", { name: "Validate" });
    const [viewBox, validateBox] = await Promise.all([viewButton.boundingBox(), validateButton.boundingBox()]);
    expect(Math.abs((viewBox?.width ?? 0) - (validateBox?.width ?? 0))).toBeLessThan(2);
    await viewButton.click();
    const restore = page.getByRole("region", { name: "Restore latest chain-valid snapshot" });
    await expect(restore.getByText("Run this command on the restore host.")).toBeVisible();
    await expect(restore.getByText("Step 1")).toHaveCount(0);
    await restore.getByRole("button", { name: "Copy command" }).click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toContain("restore@");
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toContain("restore-stream");
  });

  test("keeps administration pages usable at a narrow viewport", async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await signIn(page);

    for (const destination of ["Dashboard", "Datasets", "Activity", "Status", "Settings"]) {
      await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: destination, exact: true }).click();
      const dimensions = await page.evaluate(() => ({ width: document.documentElement.clientWidth, scrollWidth: document.documentElement.scrollWidth }));
      expect(dimensions.scrollWidth, `${destination} should not scroll horizontally`).toBeLessThanOrEqual(dimensions.width);
    }

    const settingsHeadings = await page.locator("main > section > h2").allTextContents();
    expect(settingsHeadings.at(-1)).toBe("Backup policy");
    await page.getByRole("navigation", { name: "Primary" }).getByRole("link", { name: "Datasets", exact: true }).click();
    await page.getByRole("link", { name: "View" }).first().click();
    const dimensions = await page.evaluate(() => ({ width: document.documentElement.clientWidth, scrollWidth: document.documentElement.scrollWidth }));
    expect(dimensions.scrollWidth, "Dataset details should not scroll horizontally").toBeLessThanOrEqual(dimensions.width);
  });
});
