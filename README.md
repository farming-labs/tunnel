# Farm Preview Agent (Rust)

Experimental Rust/N-API agent for Farm's persistent preview tunnel protocol.

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
} = require("@farm.js/preview-agent-rs");

const session = await startPreviewAgent(
  "ws://127.0.0.1:4400/agent",
  "my-preview",
  "http://127.0.0.1:3000",
);

console.log(session.publicUrl);
await stopPreviewAgent(session.sessionId);
```

## Why N-API instead of WASM?

The agent owns native TCP/TLS/WebSocket connections and forwards arbitrary HTTP bodies. N-API allows the Rust runtime to do that work directly while presenting a small JavaScript API. A WASM build would still need JavaScript host shims for networking and would not provide a useful comparison of the native transport.

## Initial benchmark

On an Apple M1 with a 4 KiB payload and the same loopback relay/protocol, the optimized Rust agent averaged 0.29 ms sequentially and 4.45 ms at concurrency 25. The TypeScript agent averaged 0.47 ms and 7.34 ms respectively. Rust delivered about 61–65% more throughput in this agent-overhead benchmark.

The benchmark harness and full methodology live in the sibling Farm.js repository under `benchmarks/preview-tunnel`. These numbers isolate agent overhead; a production relay benchmark must also include TLS and real network distance.
