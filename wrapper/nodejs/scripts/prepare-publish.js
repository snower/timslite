#!/usr/bin/env node

const { readFileSync, writeFileSync } = require("node:fs");
const { join, resolve } = require("node:path");

function readVersionFromCargoToml(toml) {
  const match = toml.match(/^\s*version\s*=\s*"([^"]+)"/m);
  if (!match) {
    throw new Error("Cargo.toml does not contain a package version");
  }
  return match[1];
}

function main(rootDir = resolve(__dirname, "..")) {
  const packageJsonPath = join(rootDir, "package.json");
  const packageJson = JSON.parse(readFileSync(packageJsonPath, "utf8"));
  const version = packageJson.version;

  const repoCargoToml = readFileSync(join(rootDir, "..", "..", "Cargo.toml"), "utf8");
  const crateVersion = readVersionFromCargoToml(repoCargoToml);
  if (crateVersion !== version) {
    throw new Error(`package.json version ${version} does not match root Cargo.toml version ${crateVersion}`);
  }

  const wrapperCargoTomlPath = join(rootDir, "Cargo.toml");
  const wrapperCargoToml = readFileSync(wrapperCargoTomlPath, "utf8");
  const wrapperVersion = readVersionFromCargoToml(wrapperCargoToml);
  if (wrapperVersion !== version) {
    throw new Error(`package.json version ${version} does not match wrapper Cargo.toml version ${wrapperVersion}`);
  }

  const updated = wrapperCargoToml.replace(
    /^timslite\s*=\s*\{[^}]*\}\s*$/m,
    `timslite = { version = "=${version}" }`,
  );
  if (updated === wrapperCargoToml) {
    throw new Error("Could not rewrite timslite dependency in wrapper Cargo.toml");
  }
  writeFileSync(wrapperCargoTomlPath, updated);
  console.log(`Prepared npm source fallback with timslite = "=${version}"`);
}

if (require.main === module) {
  main();
}

module.exports = {
  main,
  readVersionFromCargoToml,
};
