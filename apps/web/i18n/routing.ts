import { defaultLocale, locales } from "@sijil/i18n";
import { defineRouting } from "next-intl/routing";

export const routing = defineRouting({
  locales,
  defaultLocale,
  // The signed-in user's saved locale decides (M1); never guess from the browser.
  localeDetection: false,
});
