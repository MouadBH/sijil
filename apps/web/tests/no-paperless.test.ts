import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

import { expect, test } from "vitest";

// Hard rule: no JS package may talk to Paperless-ngx. Everything goes through the
// control-plane /v1 API, so no source file under apps/ or packages/ may name it.
const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
const roots = ["apps", "packages"].map((dir) => join(repoRoot, dir));
const skipDirs = new Set(["node_modules", ".next", "out", "playwright-report", "test-results"]);
const codeFile = /\.(ts|tsx|js|jsx|mjs|cjs)$/;
const self = fileURLToPath(import.meta.url);

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) {
      if (!skipDirs.has(entry)) yield* walk(path);
    } else if (codeFile.test(entry) && path !== self) {
      yield path;
    }
  }
}

test("no JS source references paperless", () => {
  const offenders = [...roots.flatMap((root) => [...walk(root)])]
    .filter((path) => /paperless/i.test(readFileSync(path, "utf8")))
    .map((path) => relative(repoRoot, path));
  expect(offenders).toEqual([]);
});
