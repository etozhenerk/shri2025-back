import fs from "fs";
import { writeFile, appendFile, mkdir, unlink } from "fs/promises";

export function createFileWriter(options) {
  const { OUTPUT_FILE, OUTPUT_FOLDER, HEADERS } = options;

  return {
    writeHeaders: async () => {
      if (!fs.existsSync(OUTPUT_FOLDER)) {
        await mkdir(OUTPUT_FOLDER, { recursive: true });
      }

      if (fs.existsSync(OUTPUT_FILE)) {
        await unlink(OUTPUT_FILE);
      }

      const headersString = HEADERS.join(",") + "\n";
      await writeFile(OUTPUT_FILE, headersString);
    },
    writeChunk: (data) => appendFile(OUTPUT_FILE, data),
    removeLastNewline: async () => {
      // Получаем размер файла
      const stats = fs.statSync(OUTPUT_FILE);
      const fileSize = stats.size;

      // Если файл пустой или слишком маленький, ничего не делаем
      if (fileSize <= 0) {
        return;
      }

      // Открываем файл и обрезаем последний символ, если это перенос строки
      const fd = fs.openSync(OUTPUT_FILE, "r+");
      const buffer = Buffer.alloc(1);

      // Читаем последний байт файла
      fs.readSync(fd, buffer, 0, 1, fileSize - 1);

      // Если последний символ - перенос строки, обрезаем файл
      if (buffer.toString() === "\n") {
        fs.ftruncateSync(fd, fileSize - 1);
      }

      // Закрываем файл
      fs.closeSync(fd);
    },
  };
}
