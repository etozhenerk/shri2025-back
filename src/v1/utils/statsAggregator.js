/**
 * Агрегирует данные о расходах в требуемый формат
 * @param {Array<Object>} data - Массив объектов с данными о расходах
 * @returns {Object} Агрегированные статистические данные
 */
export const aggregateStats = (data) => {
  // Группируем расходы по дням
  const spendsByDate = data.reduce((acc, row) => {
    const date = row.date;
    if (!acc[date]) {
      acc[date] = 0;
    }
    acc[date] += row.spend;
    return acc;
  }, {});

  // Группируем расходы по цивилизациям
  const spendsByCiv = data.reduce((acc, row) => {
    const civ = row.civ;
    if (!acc[civ]) {
      acc[civ] = 0;
    }
    acc[civ] += row.spend;
    return acc;
  }, {});

  // Находим дни с минимальными и максимальными расходами
  const spendsByDateEntries = Object.entries(spendsByDate);
  const [lessSpentAt, lessSpentValue] = spendsByDateEntries.reduce(
    (min, curr) => (curr[1] < min[1] ? curr : min),
    spendsByDateEntries[0]
  );
  const [bigSpentAt, bigSpentValue] = spendsByDateEntries.reduce(
    (max, curr) => (curr[1] > max[1] ? curr : max),
    spendsByDateEntries[0]
  );

  // Находим цивилизации с минимальными и максимальными расходами
  const spendsByCivEntries = Object.entries(spendsByCiv);
  const [lessSpentCiv] = spendsByCivEntries.reduce(
    (min, curr) => (curr[1] < min[1] ? curr : min),
    spendsByCivEntries[0]
  );
  const [bigSpentCiv] = spendsByCivEntries.reduce(
    (max, curr) => (curr[1] > max[1] ? curr : max),
    spendsByCivEntries[0]
  );

  // Считаем общие расходы и среднее значение
  const totalSpendGalactic = data.reduce((sum, row) => sum + row.spend, 0);
  const averageSpendGalactic = totalSpendGalactic / data.length;

  return {
    total_spend_galactic: totalSpendGalactic,
    rows_affected: data.length,
    less_spent_at: parseInt(lessSpentAt),
    big_spent_at: parseInt(bigSpentAt),
    less_spent_value: lessSpentValue,
    big_spent_value: bigSpentValue,
    average_spend_galactic: averageSpendGalactic,
    big_spent_civ: bigSpentCiv,
    less_spent_civ: lessSpentCiv,
  };
};
