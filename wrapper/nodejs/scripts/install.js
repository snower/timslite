#!/usr/bin/env node

const { copyFileSync, existsSync, mkdirSync, readFileSync } = require("node:fs");
const { join, resolve } = require("node:path");
const { spawnSync: defaultSpawnSync } = require("node:child_process");
const { getBindingCandidates, getBindingFilename } = require("../binding-target");

function isTruthy(value) {
  return value === "1" || value === "true" || value === "yes";
}

function manifestUsesPathDependency(manifest) {
  return /timslite\s*=\s*\{[^}]*path\s*=/.test(manifest);
}

function getNativeLibraryNames(platform) {
  if (platform === "win32") return ["timslite_node.dll"];
  if (platform === "darwin") return ["libtimslite_node.dylib"];
  return ["libtimslite_node.so"];
}

function findBuiltLibrary(rootDir, platform) {
  const releaseDir = join(rootDir, "target", "release");
  const candidates = getNativeLibraryNames(platform).map((name) => join(releaseDir, name));
  return candidates.find((candidate) => existsSync(candidate)) ?? null;
}

function findExistingBinding(rootDir, targets) {
  for (const target of targets) {
    const filename = getBindingFilename(target);
    if (existsSync(join(rootDir, filename))) {
      return filename;
    }
  }
  return null;
}

function main(options = {}) {
  const rootDir = resolve(options.rootDir ?? join(__dirname, ".."));
  const env = options.env ?? process.env;
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const musl = options.musl;
  const log = options.log ?? console.log;
  const spawnSync = options.spawnSync ?? defaultSpawnSync;
  const targets = getBindingCandidates({ platform, arch, musl });
  const forceBuild = isTruthy(env.TIMSLITE_BUILD_FROM_SOURCE);
  const skipBuild = isTruthy(env.TIMSLITE_SKIP_SOURCE_BUILD);

  if (targets.length === 0) {
    throw new Error(`Unsupported platform for timslite source build: ${platform}/${arch}`);
  }

  const existingBinding = findExistingBinding(rootDir, targets);
  if (existingBinding && !forceBuild) {
    log(`timslite: using prebuilt native binding ${existingBinding}`);
    return { status: "prebuilt", binding: existingBinding };
  }

  if (skipBuild) {
    log("timslite: source build skipped by TIMSLITE_SKIP_SOURCE_BUILD");
    return { status: "skipped" };
  }

  const manifestPath = join(rootDir, "Cargo.toml");
  const manifest = readFileSync(manifestPath, "utf8");
  if (manifestUsesPathDependency(manifest) && !forceBuild) {
    log("timslite: source build skipped in development checkout");
    return { status: "development-checkout" };
  }

  const targetDir = join(rootDir, "target");
  mkdirSync(targetDir, { recursive: true });
  const result = spawnSync("cargo", ["build", "--release", "--locked", "--manifest-path", manifestPath], {
    cwd: rootDir,
    env: { ...process.env, ...env, CARGO_TARGET_DIR: targetDir },
    stdio: "inherit",
  });

  if (result.error) {
    throw new Error(`timslite source build failed to start: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`timslite source build failed with exit code ${result.status}`);
  }

  const library = findBuiltLibrary(rootDir, platform);
  if (!library) {
    throw new Error(`timslite source build completed, but no native library was found in ${join(targetDir, "release")}`);
  }

  const binding = getBindingFilename(targets[0]);
  copyFileSync(library, join(rootDir, binding));
  log(`timslite: built native binding ${binding} from source`);
  return { status: "built", binding };
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  }
}

module.exports = {
  findExistingBinding,
  getNativeLibraryNames,
  main,
  manifestUsesPathDependency,
};
