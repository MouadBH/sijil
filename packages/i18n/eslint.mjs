// Shared lint rules: every user-facing string comes from the message catalogues.
// Used by apps/web and apps/site; tested in apps/web/tests/i18n-lint.test.ts.
const userFacingAttributes =
  "aria-label|aria-description|aria-placeholder|aria-roledescription|aria-valuetext|title|alt|placeholder|label";

export const i18nRules = [
  {
    files: ["**/*.tsx"],
    ignores: ["tests/**"],
    rules: {
      "react/jsx-no-literals": ["error", { noStrings: true, ignoreProps: true }],
      "no-restricted-syntax": [
        "error",
        {
          selector: `JSXAttribute[name.name=/^(${userFacingAttributes})$/] > Literal`,
          message: "User-facing attribute text must come from @sijil/i18n messages.",
        },
        {
          selector: `JSXAttribute[name.name=/^(${userFacingAttributes})$/] > JSXExpressionContainer > Literal`,
          message: "User-facing attribute text must come from @sijil/i18n messages.",
        },
      ],
    },
  },
];
