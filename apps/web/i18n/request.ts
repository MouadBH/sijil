import { defaultLocale, isLocale, messages } from "@sijil/i18n";
import { getRequestConfig } from "next-intl/server";

export default getRequestConfig(async ({ requestLocale }) => {
  const requested = await requestLocale;
  const locale = requested && isLocale(requested) ? requested : defaultLocale;
  return { locale, messages: messages[locale] };
});
