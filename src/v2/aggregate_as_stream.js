import { makeLineSplitter } from "./utils/lineSplitter.js";
import { createStreamingStatsAggregator } from "./utils/streamingStatsAggregator.js";

import { pipeline } from "node:stream/promises";

export const aggregateV2asStream = async (req, res) => {
    const lineSplitter = makeLineSplitter();
    const streamingStatsAggregator = createStreamingStatsAggregator();

    res.setHeader("Content-Type", "application/json");
    res.setHeader("Transfer-Encoding", "chunked");

    await pipeline(req, lineSplitter, streamingStatsAggregator, res);
};
