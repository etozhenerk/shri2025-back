const createRandomRate = () => {
  return 0.5 + Math.random();
};

const rates = Array.from({ length: 365 }, () => {
  return {
    humans: createRandomRate(),
    blobs: createRandomRate(),
    monsters: createRandomRate(),
  };
});

// todo - передалать в async?
// тогда надо будет подумать про кеширование
export const getExchangeRate = (date, civilization) => {
  const rate = rates[date];

  if (!rate) {
    throw new Error(`Rate not found for date ${date}`);
  }

  return rate[civilization];
};
