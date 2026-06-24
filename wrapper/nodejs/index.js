const { join } = require("node:path");
const { getBindingCandidates, getBindingFilename } = require("./binding-target");

const loadErrors = [];

function loadNativeBinding() {
  const overridePath = process.env.NAPI_RS_NATIVE_LIBRARY_PATH;
  if (overridePath) {
    try {
      return require(overridePath);
    } catch (error) {
      loadErrors.push(error);
    }
  }

  const candidates = getBindingCandidates();
  if (candidates.length === 0) {
    loadErrors.push(new Error(`Unsupported platform: ${process.platform}/${process.arch}`));
    return null;
  }

  for (const target of candidates) {
    const filename = getBindingFilename(target);
    try {
      return require(join(__dirname, filename));
    } catch (error) {
      loadErrors.push(error);
    }
  }

  return null;
}

const nativeBinding = loadNativeBinding();

if (!nativeBinding) {
  const candidates = getBindingCandidates().map(getBindingFilename).join(", ");
  const error = new Error(
    `Cannot find timslite native binding for ${process.platform}/${process.arch}. ` +
      `Expected one of: ${candidates || "no supported binding target"}. ` +
      "If this platform is not prebuilt, install with a Rust toolchain available so postinstall can build from source.",
  );
  if (loadErrors.length > 0) {
    error.cause = loadErrors.reduce((previous, current) => {
      current.cause = previous;
      return current;
    });
  }
  throw error;
}

module.exports = nativeBinding;
module.exports.Dataset = nativeBinding.Dataset;
module.exports.JournalQueue = nativeBinding.JournalQueue;
module.exports.JournalQueueConsumer = nativeBinding.JournalQueueConsumer;
module.exports.QueryIterator = nativeBinding.QueryIterator;
module.exports.QueryLengthIterator = nativeBinding.QueryLengthIterator;
module.exports.Queue = nativeBinding.Queue;
module.exports.QueueConsumer = nativeBinding.QueueConsumer;
module.exports.Store = nativeBinding.Store;
module.exports.version = nativeBinding.version;
