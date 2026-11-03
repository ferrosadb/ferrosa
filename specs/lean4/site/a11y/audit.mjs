// Accessibility audit of the built site: every page, light and dark, against WCAG 2.2 A and AA.
//
//   node audit.mjs <site-dir> [report.json]
//
// It serves <site-dir> itself on a loopback port, so it never depends on a server someone left
// running. Exits 1 when any page has a violation, and 2 when the audit could not run, so a failure
// to audit is never read as a clean result.

import { createServer } from "node:http";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { extname, join, relative, resolve, sep } from "node:path";
import { chromium } from "playwright";
import AxeBuilder from "@axe-core/playwright";

const TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa", "best-practice"];
const SCHEMES = ["light", "dark"];
const TYPES = {
  ".html": "text/html; charset=utf-8", ".css": "text/css", ".js": "text/javascript",
  ".svg": "image/svg+xml", ".json": "application/json", ".png": "image/png", ".woff2": "font/woff2",
};

async function pages(root) {
  const found = [];
  const walk = async (dir) => {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) await walk(full);
      else if (entry.name === "index.html") found.push(relative(root, dir).split(sep).join("/"));
    }
  };
  await walk(root);
  return found.sort();
}

function serve(root) {
  const server = createServer(async (req, res) => {
    let path = decodeURIComponent(new URL(req.url, "http://x").pathname);
    if (path.endsWith("/")) path += "index.html";
    const file = resolve(root, "." + path);
    if (!file.startsWith(root)) { res.writeHead(403).end(); return; }
    try {
      const body = await readFile(file);
      res.writeHead(200, { "content-type": TYPES[extname(file)] || "application/octet-stream" });
      res.end(body);
    } catch {
      res.writeHead(404).end();
    }
  });
  return new Promise((ok) => server.listen(0, "127.0.0.1", () => ok(server)));
}

async function audit(browser, base, page, scheme) {
  const context = await browser.newContext({ colorScheme: scheme, viewport: { width: 1280, height: 900 } });
  const tab = await context.newPage();
  const response = await tab.goto(`${base}/${page}${page ? "/" : ""}`, { waitUntil: "networkidle" });
  if (!response || !response.ok()) throw new Error(`${page}: HTTP ${response && response.status()}`);
  const result = await new AxeBuilder({ page: tab }).withTags(TAGS).analyze();
  await context.close();
  return result.violations.map((v) => ({
    page, scheme, id: v.id, impact: v.impact, help: v.help,
    nodes: v.nodes.length, sample: v.nodes.slice(0, 3).map((n) => n.target.join(" ")),
    summary: v.nodes[0] && v.nodes[0].failureSummary,
  }));
}

async function main() {
  const [dir, out] = process.argv.slice(2);
  if (!dir) throw new Error("usage: node audit.mjs <site-dir> [report.json]");
  const root = resolve(dir);
  const list = await pages(root);
  if (list.length === 0) throw new Error(`no pages under ${root}; build the site first`);
  const server = await serve(root);
  const base = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch(
    process.env.CHROMIUM ? { executablePath: process.env.CHROMIUM } : {});
  const findings = [];
  try {
    for (const scheme of SCHEMES) {
      for (const page of list) findings.push(...(await audit(browser, base, page, scheme)));
    }
  } finally {
    await browser.close();
    server.close();
  }
  if (out) await writeFile(out, JSON.stringify(findings, null, 2));
  const byRule = new Map();
  for (const f of findings) {
    const key = `${f.impact}\t${f.id}\t${f.scheme}`;
    const agg = byRule.get(key) || { pages: 0, nodes: 0, help: f.help, sample: f.sample, summary: f.summary };
    agg.pages += 1; agg.nodes += f.nodes;
    byRule.set(key, agg);
  }
  console.log(`audited ${list.length} pages x ${SCHEMES.length} schemes`);
  for (const [key, agg] of [...byRule].sort()) {
    console.log(`${key}\t${agg.pages} pages\t${agg.nodes} nodes\t${agg.help}\n    e.g. ${agg.sample.join(" | ")}`);
  }
  process.exit(findings.length === 0 ? 0 : 1);
}

main().catch((err) => { console.error("audit could not run:", err); process.exit(2); });
