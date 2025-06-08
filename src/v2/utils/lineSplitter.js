import { TransformStream } from "node:stream/web";

const NEW_LINE_CHAR = "\n";

export const makeLineSplitter = () => {
    let currentBuffer = "";

    let isFirstLine = true;
    const keyMap = [];

    const createRecord = (line) => {
        const lineValues = line.split(",").map((key) => key.trim());

        if (lineValues.length !== keyMap.length) {
            return null;
        }

        const result = {};

        for (let i = 0; i < lineValues.length; i++) {
            const key = keyMap[i];
            const value = lineValues[i];

            if (key === "civ") {
                if (
                    value !== "humans" &&
                    value !== "blobs" &&
                    value !== "monsters"
                ) {
                    return null;
                }
            }

            if (key === "spend") {
                if (value < 0) {
                    return null;
                }
            }

            result[key] = value;
        }

        return result;
    };

    const fillKeyMap = (chunk) => {
        chunk.split(",").forEach((key) => {
            keyMap.push(key.trim());
        });
    };

    return new TransformStream({
        transform(chunk, controller) {
            currentBuffer += chunk.toString();
            let newLineIndex = currentBuffer.indexOf(NEW_LINE_CHAR);

            while (newLineIndex !== -1) {
                const line = currentBuffer.slice(0, newLineIndex);

                if (line.length) {
                    if (isFirstLine) {
                        isFirstLine = false;

                        fillKeyMap(line);
                    } else {
                        const record = createRecord(line);

                        if (record) {
                            controller.enqueue(record);
                        }
                    }
                }

                currentBuffer = currentBuffer.slice(newLineIndex + 1);
                newLineIndex = currentBuffer.indexOf(NEW_LINE_CHAR);
            }
        },
        flush(controller) {
            if (currentBuffer.length > 0) {
                const record = createRecord(currentBuffer);

                controller.enqueue(record);
            }
        },
    });
};
