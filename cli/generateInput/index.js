import path from "path";
import { createLogger } from "./utils/logger.js";
import { createRecordGenerator } from "./utils/recordGenerator.js";
import { createSizeHandler } from "./utils/sizeHandler.js";
import { createFileWriter } from "./utils/fileWriter.js";

import { generateCsv } from "./generate.js";

const TARGET_SIZE_GB = process.argv[2] && parseFloat(process.argv[2]);

if (!TARGET_SIZE_GB) {
  console.error("Target size in GB is required");
  process.exit(1);
}

const OUTPUT_FOLDER = "files";
const OUTPUT_FILE = path.join(OUTPUT_FOLDER, "input.csv");
const TARGET_SIZE_BYTES = TARGET_SIZE_GB * 1024 * 1024 * 1024;
const HEADERS = ["id", "civ", "developer_id", "date", "spend"];

const recordGenerator = createRecordGenerator();
const logger = createLogger({ TARGET_SIZE_BYTES, TARGET_SIZE_GB });
const sizeHandler = createSizeHandler({ TARGET_SIZE_BYTES, OUTPUT_FILE });
const fileWriter = createFileWriter({ OUTPUT_FILE, OUTPUT_FOLDER, HEADERS });

generateCsv({
  recordGenerator,
  logger,
  sizeHandler,
  fileWriter,
}).catch((err) => {
  console.error("Unhandled error:", err);
  process.exit(1);
});
