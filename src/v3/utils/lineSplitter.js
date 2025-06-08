import { TransformStream } from "node:stream/web";

const LINE_DELIMITER_CODE = 10; // \n

export const makeLineSplitter = () => {
  let currentBuffer = Buffer.alloc(0);

  return new TransformStream({
    transform(chunk, controller) {
      currentBuffer = Buffer.concat([currentBuffer, chunk]);
      let newLineIndex = currentBuffer.indexOf(LINE_DELIMITER_CODE);

      while (newLineIndex !== -1) {
        const line = currentBuffer.subarray(0, newLineIndex);
        controller.enqueue(line);

        currentBuffer = currentBuffer.subarray(newLineIndex + 1);
        newLineIndex = currentBuffer.indexOf(LINE_DELIMITER_CODE);
      }
    },

    flush(controller) {
      if (currentBuffer.length > 0) {
        controller.enqueue(currentBuffer);
      }
    },
  });
};
