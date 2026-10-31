import { FlatCompat } from "@eslint/eslintrc";
import { i18nRules } from "@sijil/i18n/eslint";

const compat = new FlatCompat({ baseDirectory: import.meta.dirname });

const config = [
  {
    ignores: [".next/**", "out/**", "next-env.d.ts", "playwright-report/**", "test-results/**"],
  },
  ...compat.extends("next/core-web-vitals", "next/typescript"),
  ...i18nRules,
];

export default config;
