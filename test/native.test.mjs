import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";

const require = createRequire(import.meta.url);
const agent = require("../index.js");

test("loads the Rust N-API preview agent", () => {
  assert.equal(typeof agent.startPreviewAgent, "function");
  assert.equal(typeof agent.stopPreviewAgent, "function");
  assert.equal(agent.activePreviewAgentCount(), 0);
});
