function bytesToMB(bytes) {
  return (bytes / 1024 / 1024).toFixed(2);
}

function bytesToGB(bytes) {
  return (bytes / 1024 / 1024 / 1024).toFixed(2);
}

export const createLogger = (options) => {
  const { TARGET_SIZE_BYTES, TARGET_SIZE_GB } = options;

  let lastLogTime = Date.now();
  let startTime = Date.now();

  const logStart = () => {
    console.log(`Starting CSV file generation`);
  };

  const logProgress = (currentSize) => {
    const now = Date.now();

    if (now - lastLogTime > 10) {
      const progressPercent = ((currentSize / TARGET_SIZE_BYTES) * 100).toFixed(
        2
      );
      const elapsedSeconds = Math.floor((now - startTime) / 1000);
      const mbGenerated = bytesToMB(currentSize);

      // Calculate speed and estimated time remaining
      const speedMBPerSec = mbGenerated / elapsedSeconds;
      const remainingBytes = TARGET_SIZE_BYTES - currentSize;
      const remainingMB = bytesToMB(remainingBytes);
      const remainingSeconds = remainingMB / speedMBPerSec;

      // Format remaining time
      const remainingMinutes = Math.floor(remainingSeconds / 60);
      const remSec = Math.floor(remainingSeconds % 60);

      // Output progress
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      process.stdout.write(
        `Progress: ${progressPercent}% (${mbGenerated} MB of ${TARGET_SIZE_GB} GB). ` +
          `Speed: ${speedMBPerSec.toFixed(2)} MB/s. ` +
          `Time remaining: ${remainingMinutes}m ${remSec}s`
      );

      lastLogTime = now;
    }
  };

  const logResult = (finalSize, totalRecords) => {
    const finalSizeGB = bytesToGB(finalSize);
    const totalTime = Math.floor((Date.now() - startTime) / 1000);
    const minutes = Math.floor(totalTime / 60);
    const seconds = totalTime % 60;

    console.log("\n");
    console.log(`Generation completed! File size: ${finalSizeGB} GB`);
    console.log(`Time elapsed: ${minutes}m ${seconds}s`);
    console.log(`Records generated: approximately ${totalRecords}`);
  };

  const logError = (error) => {
    console.error("Error during file generation:", error);
  };

  return {
    logStart,
    logProgress,
    logResult,
    logError,
  };
};
