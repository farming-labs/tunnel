import { access, readFile } from "node:fs/promises";
import { join } from "node:path";

const targetPlatforms = new Map([
  ["aarch64-apple-darwin", "darwin-arm64"],
  ["x86_64-apple-darwin", "darwin-x64"],
  ["aarch64-unknown-linux-gnu", "linux-arm64-gnu"],
  ["aarch64-unknown-linux-musl", "linux-arm64-musl"],
  ["x86_64-unknown-linux-gnu", "linux-x64-gnu"],
  ["x86_64-unknown-linux-musl", "linux-x64-musl"],
  ["aarch64-pc-windows-msvc", "win32-arm64-msvc"],
  ["x86_64-pc-windows-msvc", "win32-x64-msvc"],
]);

const root = new URL("../", import.meta.url);
const packageJson = JSON.parse(
  await readFile(new URL("package.json", root), "utf8"),
);
const missing = [];

for (const target of packageJson.napi.targets) {
  const platform = targetPlatforms.get(target);
  if (!platform) {
    missing.push(`No artifact mapping exists for ${target}`);
    continue;
  }

  const binary = `${packageJson.napi.binaryName}.${platform}.node`;
  const platformDirectory = new URL(`npm/${platform}/`, root);
  const platformPackage = JSON.parse(
    await readFile(new URL("package.json", platformDirectory), "utf8"),
  );
  const expectedPackage = `${packageJson.name}-${platform}`;

  if (platformPackage.name !== expectedPackage) {
    missing.push(
      `${platform}/package.json is named ${platformPackage.name}, expected ${expectedPackage}`,
    );
  }
  if (platformPackage.main !== binary) {
    missing.push(
      `${platform}/package.json points to ${platformPackage.main}, expected ${binary}`,
    );
  }
  if (
    packageJson.optionalDependencies?.[expectedPackage] !== packageJson.version
  ) {
    missing.push(`${expectedPackage} is not pinned to ${packageJson.version}`);
  }

  for (const artifact of [
    new URL(binary, root),
    new URL(binary, platformDirectory),
  ]) {
    try {
      await access(artifact);
    } catch {
      missing.push(join(artifact.pathname));
    }
  }
}

if (missing.length) {
  throw new Error(
    `Release artifacts are incomplete:\n- ${missing.join("\n- ")}`,
  );
}

console.log(
  `Verified ${packageJson.napi.targets.length} native platform artifacts.`,
);
