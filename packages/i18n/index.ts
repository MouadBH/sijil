import ar from "./messages/ar.json" with { type: "json" };
import en from "./messages/en.json" with { type: "json" };
import fr from "./messages/fr.json" with { type: "json" };

export const locales = ["fr", "ar", "en"] as const;
export type Locale = (typeof locales)[number];
export const defaultLocale: Locale = "fr";

const rtlLocales: ReadonlySet<Locale> = new Set<Locale>(["ar"]);

export function dirFor(locale: Locale): "rtl" | "ltr" {
  return rtlLocales.has(locale) ? "rtl" : "ltr";
}

export function isLocale(value: string): value is Locale {
  return (locales as readonly string[]).includes(value);
}

export type Messages = typeof fr;
export const messages: Record<Locale, Messages> = { fr, ar, en };
