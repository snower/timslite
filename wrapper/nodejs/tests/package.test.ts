import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

function readPackageJson(): any {
  return JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
}

function tempPackageRoot(): string {
  return mkdtempSync(join(tmpdir(), "timslite-node-package-test-"));
}

describe("package metadata", () => {
  it("ships README and source fallback files in the npm package", () => {
    const pkg = readPackageJson();

    assert(pkg.files.includes("README.md"));
    assert(pkg.files.includes("Cargo.toml"));
    assert(pkg.files.includes("Cargo.lock"));
    assert(pkg.files.includes("build.rs"));
    assert(pkg.files.includes("src/**"));
    assert(pkg.files.includes("scripts/**"));
  });

  it("build scripts preserve the checked-in loader", () => {
    const pkg = readPackageJson();

    assert.match(pkg.scripts.build, /--no-js/);
    assert.match(pkg.scripts["build:debug"], /--no-js/);
  });
});

describe("runtime loader", () => {
  it("does not fall back to unpublished platform packages", () => {
    const loader = readFileSync(new URL("../index.js", import.meta.url), "utf8");

    assert.doesNotMatch(loader, /bindingPackageVersion/);
    assert.doesNotMatch(loader, /require\(['"]timslite-/);
  });
});

describe("postinstall source build fallback", () => {
  it("skips source build in the development checkout", () => {
    const install = require("../scripts/install.js");
    const rootDir = tempPackageRoot();
    writeFileSync(join(rootDir, "Cargo.toml"), 'timslite = { path = "../..", version = "=0.1.4" }\n');

    let spawnCalls = 0;
    const result = install.main({
      rootDir,
      env: {},
      platform: "linux",
      arch: "x64",
      musl: false,
      log: () => undefined,
      spawnSync: () => {
        spawnCalls += 1;
        return { status: 0 };
      },
    });

    assert.equal(result.status, "development-checkout");
    assert.equal(spawnCalls, 0);
  });

  it("builds from source and writes the current platform binding when no prebuild exists", () => {
    const install = require("../scripts/install.js");
    const rootDir = tempPackageRoot();
    writeFileSync(join(rootDir, "Cargo.toml"), 'timslite = { version = "=0.1.4" }\n');

    const releaseDir = join(rootDir, "target", "release");
    mkdirSync(releaseDir, { recursive: true });

    const result = install.main({
      rootDir,
      env: {},
      platform: "linux",
      arch: "x64",
      musl: false,
      log: () => undefined,
      spawnSync: () => {
        writeFileSync(join(releaseDir, "libtimslite_node.so"), "native");
        return { status: 0 };
      },
    });

    assert.equal(result.status, "built");
    assert.equal(result.binding, "timslite.linux-x64-gnu.node");
    assert.equal(readFileSync(join(rootDir, "timslite.linux-x64-gnu.node"), "utf8"), "native");
  });
});
