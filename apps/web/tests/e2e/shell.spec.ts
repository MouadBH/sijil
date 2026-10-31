import { expect, test } from "@playwright/test";

import { messages } from "@sijil/i18n";

test("root redirects to the default locale", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/fr$/);
});

for (const [locale, dir] of [
  ["fr", "ltr"],
  ["ar", "rtl"],
  ["en", "ltr"],
] as const) {
  test(`/${locale} renders ${dir} with translated heading`, async ({ page }) => {
    await page.goto(`/${locale}`);
    const html = page.locator("html");
    await expect(html).toHaveAttribute("lang", locale);
    await expect(html).toHaveAttribute("dir", dir);
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(
      messages[locale].shell.title,
    );
  });
}
