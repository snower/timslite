const { execSync } = require("node:child_process");
const { readFileSync } = require("node:fs");

function isFileMusl(file) {
  return file.includes("libc.musl-") || file.includes("ld-musl-");
}

function isMuslFromFilesystem() {
  try {
    return readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {
    return null;
  }
}

function isMuslFromReport() {
  let report = null;
  if (process.report && typeof process.report.getReport === "function") {
    process.report.excludeNetwork = true;
    report = process.report.getReport();
  }
  if (!report) {
    return null;
  }
  if (report.header && report.header.glibcVersionRuntime) {
    return false;
  }
  if (Array.isArray(report.sharedObjects) && report.sharedObjects.some(isFileMusl)) {
    return true;
  }
  return false;
}

function isMuslFromChildProcess() {
  try {
    return execSync("ldd --version", { encoding: "utf8" }).includes("musl");
  } catch {
    return false;
  }
}

function isMusl() {
  if (process.platform !== "linux") {
    return false;
  }
  return isMuslFromFilesystem() ?? isMuslFromReport() ?? isMuslFromChildProcess();
}

function getBindingCandidates(options = {}) {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const musl = options.musl ?? (platform === "linux" ? isMusl() : false);

  if (platform === "darwin") {
    if (arch === "x64") return ["darwin-x64"];
    if (arch === "arm64") return ["darwin-arm64"];
    return [];
  }

  if (platform === "win32") {
    if (arch === "x64") return ["win32-x64-msvc"];
    if (arch === "arm64") return ["win32-arm64-msvc"];
    if (arch === "ia32") return ["win32-ia32-msvc"];
    return [];
  }

  if (platform === "linux") {
    if (arch === "x64") return [musl ? "linux-x64-musl" : "linux-x64-gnu"];
    if (arch === "arm64") return [musl ? "linux-arm64-musl" : "linux-arm64-gnu"];
    if (arch === "arm") return [musl ? "linux-arm-musleabihf" : "linux-arm-gnueabihf"];
    if (arch === "loong64") return [musl ? "linux-loong64-musl" : "linux-loong64-gnu"];
    if (arch === "riscv64") return [musl ? "linux-riscv64-musl" : "linux-riscv64-gnu"];
    if (arch === "ppc64") return ["linux-ppc64-gnu"];
    if (arch === "s390x") return ["linux-s390x-gnu"];
    return [];
  }

  if (platform === "freebsd") {
    if (arch === "x64") return ["freebsd-x64"];
    if (arch === "arm64") return ["freebsd-arm64"];
    return [];
  }

  if (platform === "android") {
    if (arch === "arm64") return ["android-arm64"];
    if (arch === "arm") return ["android-arm-eabi"];
    return [];
  }

  return [];
}

function getBindingFilename(target) {
  return `timslite.${target}.node`;
}

module.exports = {
  getBindingCandidates,
  getBindingFilename,
  isMusl,
};
