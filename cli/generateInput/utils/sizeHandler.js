import { stat } from "fs/promises";

export function createSizeHandler(options) {
  const { TARGET_SIZE_BYTES, OUTPUT_FILE } = options;
  let currentSize = 0;

  const getSize = async () => {
    const { size } = await stat(OUTPUT_FILE);

    return size;
  };

  const init = async () => {
    const size = await getSize();
    currentSize = size;
  };

  const updateSize = (size) => {
    currentSize += size;

    return currentSize;
  };

  const isReady = () => currentSize >= TARGET_SIZE_BYTES;

  return {
    getSize,
    init,
    updateSize,
    isReady,
  };
}
