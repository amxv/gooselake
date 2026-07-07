import assert from "node:assert/strict";
import {
  developmentTicketRequestBody,
  mintDevelopmentTicket
} from "../app/realtime/client";

const activeOrigin = "http://127.0.0.1:13000";

assert.deepEqual(developmentTicketRequestBody(activeOrigin), {
  allowed_origins: [activeOrigin]
});

assert.deepEqual(
  developmentTicketRequestBody(activeOrigin, [
    "http://localhost:3000",
    activeOrigin,
    " "
  ]),
  {
    allowed_origins: [activeOrigin, "http://localhost:3000"]
  }
);

assert.deepEqual(developmentTicketRequestBody(undefined, []), {});

let capturedRoute: unknown;
let capturedBody: unknown;

Object.defineProperty(globalThis, "window", {
  configurable: true,
  value: {
    location: {
      origin: activeOrigin
    }
  }
});

globalThis.fetch = async (input, init) => {
  capturedRoute = input;
  capturedBody = JSON.parse(String(init?.body ?? "{}"));
  return new Response(JSON.stringify({ ticket: "dev-ticket-from-test" }), {
    headers: {
      "content-type": "application/json"
    },
    status: 200
  });
};

const ticket = await mintDevelopmentTicket();

assert.equal(ticket, "dev-ticket-from-test");
assert.equal(capturedRoute, "/api/dev-ticket");
assert.deepEqual(capturedBody, {
  allowed_origins: [activeOrigin]
});

console.log("development ticket smoke fixture passed");
