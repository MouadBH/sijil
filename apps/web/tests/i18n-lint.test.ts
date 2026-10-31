import { fileURLToPath } from "node:url";

import { ESLint } from "eslint";
import { describe, expect, test } from "vitest";

// The "no hard-coded user-facing strings" rule must hold for every .tsx file,
// including accessible names screen readers announce.
const cwd = fileURLToPath(new URL("..", import.meta.url));
const eslint = new ESLint({ cwd });

async function errors(code: string, filePath: string): Promise<string[]> {
  const [result] = await eslint.lintText(code, { filePath });
  return (result?.messages ?? []).filter((m) => m.severity === 2).map((m) => m.ruleId ?? "");
}

describe("i18n lint", () => {
  test("text literal is an error outside app/ too", async () => {
    const code = "export function X() { return <p>Bonjour</p>; }\n";
    expect(await errors(code, "src/features/x.tsx")).toContain("react/jsx-no-literals");
  });

  test.each(["aria-label", "title", "alt", "placeholder", "aria-description", "label"])(
    "literal %s attribute is an error",
    async (attr) => {
      const code = `export function X() { return <input ${attr}="Rechercher" />; }\n`;
      expect(await errors(code, "components/x.tsx")).toContain("no-restricted-syntax");
    },
  );

  test("non-user-facing attributes may be literals", async () => {
    const code = 'export function X() { return <div className="row" data-testid="r" />; }\n';
    expect(await errors(code, "components/x.tsx")).toEqual([]);
  });
});
