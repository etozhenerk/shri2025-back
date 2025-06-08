import { makeLineSplitter } from "./utils/lineSplitter.js";
import { pipeline } from "node:stream/promises";

export const aggregateV4 = async (req, res) => {
    const lineSplitter = makeLineSplitter();

    res.setHeader("Content-Type", "application/json");

    const writer = lineSplitter.writable.getWriter();

    req.on("end", () => {
        writer.close();
    });

    let maxMemoryUsage = 0;

    const handleChunk = () => {
        req.once("data", (chunk) => {
            writer.write(chunk).then(() => {
                if (process.memoryUsage().heapUsed > maxMemoryUsage) {
                    maxMemoryUsage = process.memoryUsage().heapUsed;
                }

                handleChunk();
            });
        });
    };

    handleChunk();

    await pipeline(lineSplitter, res);
};
