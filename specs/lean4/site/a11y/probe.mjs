// Detail for one rule on one page: every failing node's selector, markup and axe message.
//
//   node probe.mjs <site-dir> <page> <rule-id> [light|dark]
//
// The audit groups by rule; this is how to see what a group is made of before fixing it.

import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, resolve } from "node:path";
import { chromium } from "playwright";
import AxeBuilder from "@axe-core/playwright";

const TYPES = { ".html": "text/html; charset=utf-8", ".css": "text/css", ".js": "text/javascript", ".svg": "image/svg+xml" };

async function main() {
  const [dir, page, rule, scheme = "light"] = process.argv.slice(2);
  if (!rule) throw new Error("usage: node probe.mjs <site-dir> <page> <rule-id> [light|dark]");
  const root = resolve(dir);
  const server = createServer(async (req, res) => {
    let path = decodeURIComponent(new URL(req.url, "http://x").pathname);
    if (path.endsWith("/")) path += "index.html";
    const file = resolve(root, "." + path);
    try {
      res.writeHead(200, { "content-type": TYPES[extname(file)] || "application/octet-stream" });
      res.end(await readFile(file));
    } catch {
      res.writeHead(404).end();
    }
  });
  await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
  const browser = await chromium.launch(process.env.CHROMIUM ? { executablePath: process.env.CHROMIUM } : {});
  try {
    const context = await browser.newContext({ colorScheme: scheme });
    const tab = await context.newPage();
    await tab.goto(`http://127.0.0.1:${server.address().port}/${page}/`, { waitUntil: "networkidle" });
    const result = await new AxeBuilder({ page: tab }).withRules([rule]).analyze();
    const nodes = result.violations.flatMap((v) => v.nodes);
    console.log(`${nodes.length} failing nodes`);
    for (const n of nodes.slice(0, 8)) {
      console.log(`- ${n.target.join(" ")}\n  ${n.html.slice(0, 240)}\n  ${(n.failureSummary || "").split("\n").slice(1).join(" ")}`);
    }
  } finally {
    await browser.close();
    server.close();
  }
}

main().catch((err) => { console.error("probe could not run:", err); process.exit(2); });
