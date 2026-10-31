import { describe, expect, test } from "vitest";

import { defaultLocale, dirFor, isLocale, locales, messages } from "../index";

type Tree = { [key: string]: string | Tree };

function flatten(tree: Tree, prefix = ""): Map<string, string> {
  const out = new Map<string, string>();
  for (const [key, value] of Object.entries(tree)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof value === "string") out.set(path, value);
    else for (const [k, v] of flatten(value, path)) out.set(k, v);
  }
  return out;
}

describe("message catalogues", () => {
  const flat = Object.fromEntries(locales.map((l) => [l, flatten(messages[l] as Tree)]));
  const reference = [...flat[defaultLocale]!.keys()].sort();

  test.each(locales)("%s has exactly the same keys as fr", (locale) => {
    expect([...flat[locale]!.keys()].sort()).toEqual(reference);
  });

  test.each(locales)("%s has no empty values", (locale) => {
    const empty = [...flat[locale]!].filter(([, v]) => v.trim() === "").map(([k]) => k);
    expect(empty).toEqual([]);
  });
});

describe("locales", () => {
  test("fr is the default, ar is the only rtl locale", () => {
    expect(defaultLocale).toBe("fr");
    expect(locales.map(dirFor)).toEqual(["ltr", "rtl", "ltr"]);
  });

  test("isLocale accepts only configured locales", () => {
    expect(isLocale("ar")).toBe(true);
    expect(isLocale("de")).toBe(false);
  });
});
