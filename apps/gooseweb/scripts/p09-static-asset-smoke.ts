import assert from "node:assert/strict";
import { createServer as createHttpServer } from "node:http";
import { createServer } from "vite";

const vite = await createServer({
  configFile: false,
  root: new URL("..", import.meta.url).pathname,
  appType: "custom",
  logLevel: "silent",
  server: { middlewareMode: true }
});
const http = createHttpServer((request, response) => {
  vite.middlewares(request, response, (error) => {
    response.statusCode = error ? 500 : 404;
    response.end(error instanceof Error ? error.message : "not found");
  });
});
await new Promise<void>((resolve, reject) => {
  http.once("error", reject);
  http.listen(0, "127.0.0.1", resolve);
});
try {
  const address = http.address();
  assert.ok(address && typeof address === "object");
  const response = await fetch(`http://127.0.0.1:${address.port}/favicon.svg`);
  assert.equal(response.status, 200, "the same-origin favicon route must be successful");
  assert.match(response.headers.get("content-type") ?? "", /image\/svg\+xml/);
  assert.match(await response.text(), /<svg[^>]+Gooseweb/);
} finally {
  await new Promise<void>((resolve, reject) => http.close((error) => error ? reject(error) : resolve()));
  await vite.close();
}

console.log("P09 same-origin favicon route returns 2xx");
