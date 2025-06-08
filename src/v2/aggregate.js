import { makeLineSplitter } from "./utils/lineSplitter.js";
import { createStatsAggregator } from "./utils/statsAggregator.js";
import { pipeline } from "node:stream/promises";

export const aggregateV2 = async (req, res) => {
    const lineSplitter = makeLineSplitter();
    const statsAggregator = createStatsAggregator();

    res.setHeader("Content-Type", "application/json");
    await pipeline(req, lineSplitter, statsAggregator, res);
};
