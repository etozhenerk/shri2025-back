import { makeLineSplitter } from "./utils/lineSplitter.js";
import { createStreamingStatsAggregator } from "./utils/streamingStatsAggregator.js";

import { pipeline } from "node:stream/promises";

export const aggregateV3asStream = async (req, res) => {
    const lineSplitter = makeLineSplitter();
    const streamingStatsAggregator = createStreamingStatsAggregator();

    await pipeline(req, lineSplitter, streamingStatsAggregator, res);
};
