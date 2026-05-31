import asc from "assemblyscript/asc";
// Exposed to the Rust host: compile AS source string -> Uint8Array (wasm) or throw.
globalThis.__ascCompile = async function (src) {
  let out = null;
  const r = await asc.main(["i.ts","--outFile","o.wasm","-O0","--runtime","stub","--enable","mutable-globals","--use","abort="], {
    readFile: (n) => n === "i.ts" ? src : null,
    writeFile: (n, c) => { if (n === "o.wasm") out = c; },
    listFiles: () => [],
  });
  if (r.error) throw new Error("asc: " + r.error.message + "\n" + (r.stderr ? r.stderr.toString() : ""));
  return out;
};
