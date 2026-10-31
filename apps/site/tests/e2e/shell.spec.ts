import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { expect, test } from "@playwright/test";

import { locales, messages } from "@sijil/i18n";

const outDir = fileURLToPath(new URL("../../out", import.meta.url));

test("static export writes one page per locale", () => {
  for (const locale of locales) {
    expect(existsSync(`${outDir}/${locale}/index.html`), locale).toBe(true);
  }
});

for (const [locale, dir] of [
  ["fr", "ltr"],
  ["ar", "rtl"],
  ["en", "ltr"],
] as const) {
  test(`/${locale}/ renders ${dir} with translated hero`, async ({ page }) => {
    await page.goto(`/${locale}/`);
    const html = page.locator("html");
    await expect(html).toHaveAttribute("lang", locale);
    await expect(html).toHaveAttribute("dir", dir);
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(
      messages[locale].site.hero.title,
    );
    await expect(page).toHaveTitle(messages[locale].site.meta.title);
  });
}
