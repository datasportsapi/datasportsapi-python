// Fails when any file under the given directories holds something shaped like a
// real key or secret. Run before every publish (sdk/notes.md → Secrets); the
// public repos' workflows carry a copy (scan-secrets.mjs at their root).
//
//   node sdk/gen/scan-secrets.mjs sdk/js sdk/python sdk/go
//
// A match prints the file, line and the matched prefix only, never the value.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const PATTERNS = [
  { name: "secret API key", re: /\bsk_(?:live|test)_[A-Za-z0-9]{8,}/ },
  { name: "publishable key", re: /\bpk_(?:live|test)_[A-Za-z0-9]{8,}/ },
  { name: "reseller key", re: /\brs_(?:live|test)_[A-Za-z0-9]{8,}/ },
  { name: "webhook secret", re: /\bwhsec_[A-Za-z0-9]{8,}/ },
  { name: "private key block", re: /-----BEGIN [A-Z ]*PRIVATE KEY-----/ },
  { name: "GitHub token", re: /\bgh[pousr]_[A-Za-z0-9]{20,}/ },
  { name: "npm token", re: /\bnpm_[A-Za-z0-9]{30,}/ },
  { name: "PyPI token", re: /\bpypi-[A-Za-z0-9_-]{30,}/ },
  { name: "AWS key id", re: /\bAKIA[0-9A-Z]{16}\b/ },
];
const SKIP_DIRS = new Set([".git", "node_modules", ".venv", "dist", "__pycache__", ".pytest_cache"]);
const MAX = 8 << 20;

const roots = process.argv.slice(2);
if (!roots.length) {
  console.error("usage: node scan-secrets.mjs <dir> [<dir> …]");
  process.exit(2);
}
let found = 0;
let files = 0;
const walk = (dir) => {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) {
      if (!SKIP_DIRS.has(name)) walk(p);
      continue;
    }
    if (!st.isFile() || st.size > MAX) continue;
    files++;
    const lines = readFileSync(p, "latin1").split("\n");
    lines.forEach((line, i) => {
      for (const { name: what, re } of PATTERNS) {
        const m = re.exec(line);
        if (m) {
          found++;
          console.error(`✗ ${relative(process.cwd(), p)}:${i + 1}: ${what} (${m[0].slice(0, 8)}…)`);
        }
      }
    });
  }
};
for (const r of roots) walk(r);
if (found) {
  console.error(`scan-secrets: ${found} possible secret(s) in ${files} files — nothing may be published`);
  process.exit(1);
}
console.log(`scan-secrets: ${files} files, nothing shaped like a key`);
