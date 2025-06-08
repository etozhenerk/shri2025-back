import { ReadableStream, WritableStream, TransformStream } from "stream/web";
import { pipeline } from "stream/promises";

export async function generateCsv(options) {
  const { logger, recordGenerator, sizeHandler, fileWriter } = options;

  const sourceStream = new ReadableStream({
    pull(controller) {
      if (sizeHandler.isReady()) {
        controller.close();
        return;
      }

      // Генерируем блок записей
      const recordsBlock = recordGenerator.generateRecordsBlock(100);

      controller.enqueue(recordsBlock);
    },
  });

  // Трансформирующий поток для отслеживания размера
  const sizeTrackerTransform = new TransformStream({
    transform(chunk, controller) {
      const nextSize = sizeHandler.updateSize(Buffer.byteLength(chunk));

      logger.logProgress(nextSize);

      controller.enqueue(chunk);
    },
  });

  // Создаем поток для записи в файл
  const fileWritableStream = new WritableStream({
    async write(chunk) {
      await fileWriter.writeChunk(chunk);
    },
  });

  try {
    logger.logStart();

    await fileWriter.writeHeaders();
    await sizeHandler.init();

    await sourceStream
      .pipeThrough(sizeTrackerTransform)
      .pipeTo(fileWritableStream);

    await fileWriter.removeLastNewline();
    const finalSize = await sizeHandler.getSize();

    const totalRecords = recordGenerator.getTotalRecords();
    logger.logResult(finalSize, totalRecords);
  } catch (err) {
    logger.logError(err);
    process.exit(1);
  }
}
