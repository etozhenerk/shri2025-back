import { createAggregator } from "./utils/aggregator.js";
import { getExchangeRate } from "../exhangeRates.js";

export const aggregateV1 = async (req, res) => {
  const aggregator = createAggregator({ getExchangeRate });

  req.on("data", aggregator.addText);

  req.on("end", () => {
    const values = aggregator.aggregate();

    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify(values));
  });
};
