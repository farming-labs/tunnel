# `@farm.js/tunnel`

Experimental implementation of Farm's persistent preview tunnel protocol.

The agent opens one outbound WebSocket to a Farm preview relay, receives multiplexed HTTP requests, forwards them to a local app, and returns responses over the same connection. It is intentionally stored outside the Farm.js monorepo so it can be released as an optional native package.

## Build

```bash
npm install
npm run build
```

## Node API

```js
const {
  startPreviewAgent,
  stopPreviewAgent,
  waitPreviewAgent,
  waitPreviewAgentExit,
} = require("@farm.js/tunnel");

const session = await startPreviewAgent(
  "ws://127.0.0.1:4400/agent",
  "my-preview",
  "http://127.0.0.1:3000",
);

console.log(session.publicUrl);
const exit = await waitPreviewAgentExit(session.sessionId);
if (exit?.closeCode === 1000 && exit.closeReason === "Preview expired") {
  console.log("The hosted preview reached its expiry.");
}
```

Call `stopPreviewAgent(session.sessionId)` during explicit shutdown. `waitPreviewAgent(session.sessionId)` resolves when either the relay or local target closes the session, allowing a CLI process to share the tunnel lifecycle.
Use `waitPreviewAgentExit(session.sessionId)` instead when the caller needs the relay's WebSocket
close code and reason. It returns `null` when the session is not registered and otherwise returns an
object whose close details are present when the relay sent a close frame. Both wait functions consume
the same completed session, so call one of them per session.

The agent buffers response bodies only up to the limit advertised by the relay (5 MiB by default). It drops the upstream body immediately and returns a `502` response if the local app exceeds that limit.

When the relay reports that a public visitor disconnected or a request timed out, the agent aborts only the matching localhost request. Other concurrent preview traffic continues normally.

## Why N-API instead of WASM?

The agent owns native TCP/TLS/WebSocket connections and forwards arbitrary HTTP bodies. N-API allows the Rust runtime to do that work directly while presenting a small JavaScript API. A WASM build would still need JavaScript host shims for networking and would not provide a useful comparison of the native transport.

## Initial benchmark

On an Apple M1 with a 4 KiB payload and the same loopback relay/protocol, the optimized Rust agent averaged 0.29 ms sequentially and 4.45 ms at concurrency 25. The TypeScript agent averaged 0.47 ms and 7.34 ms respectively. Rust delivered about 61–65% more throughput in this agent-overhead benchmark.

The benchmark harness and full methodology live in the sibling Farm.js repository under `benchmarks/preview-tunnel`. These numbers isolate agent overhead; a production relay benchmark must also include TLS and real network distance.
