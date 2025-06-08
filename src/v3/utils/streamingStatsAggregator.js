import { TransformStream } from "node:stream/web";

const COMMA_DELIMITER_CODE = 44; // ","

const subarrayToString = (buffer, idx) =>
  buffer.subarray(0, idx).toString().trim();

/**
 * Создает трансформ-стрим для агрегации статистики по расходам
 * @returns {TransformStream} Трансформ-стрим для агрегации данных
 */
export const createStreamingStatsAggregator = () => {
  // Структуры для агрегации данных на лету
  const spendsByDate = {};
  const spendsByCiv = {};
  let rowCount = 0;
  let totalSpendGalactic = 0;

  let isFirstLine = true;
  const keyMap = [];

  const fillKeyMap = (chunk) => {
    let commaIndex = chunk.indexOf(COMMA_DELIMITER_CODE);

    while (commaIndex !== -1) {
      keyMap.push(subarrayToString(chunk, commaIndex));

      chunk = chunk.subarray(commaIndex + 1);
      commaIndex = chunk.indexOf(COMMA_DELIMITER_CODE);
    }
  };

  const handleRecord = (chunk) => {
    let commaIndex = chunk.indexOf(COMMA_DELIMITER_CODE);
    let currentIndex = 0;

    let spend = 0;
    let date = 0;
    let civ = 0;

    while (commaIndex !== -1) {
      if (currentIndex === spendIndex) {
        spend = Number(subarrayToString(chunk, commaIndex));
      }
      if (currentIndex === dateIndex) {
        date = subarrayToString(chunk, commaIndex);
      }
      if (currentIndex === civIndex) {
        civ = subarrayToString(chunk, commaIndex);
      }

      currentIndex++;
      chunk = chunk.subarray(commaIndex + 1);
      commaIndex = chunk.indexOf(COMMA_DELIMITER_CODE);
    }

    // Добавляем расходы в общую сумму
    totalSpendGalactic += spend;

    // Группируем расходы по дням
    if (!spendsByDate[date]) {
      spendsByDate[date] = 0;
    }
    spendsByDate[date] += spend;

    // Группируем расходы по цивилизациям
    if (!spendsByCiv[civ]) {
      spendsByCiv[civ] = 0;
    }

    spendsByCiv[civ] += spend;
  };

  const aggregateStats = () => {
    // Когда все данные получены, формируем итоговую статистику

    // Находим дни с минимальными и максимальными расходами
    const spendsByDateEntries = Object.entries(spendsByDate);

    // Проверяем, что есть хотя бы одна запись
    if (spendsByDateEntries.length === 0) {
      controller.enqueue({
        total_spend_galactic: 0,
        rows_affected: 0,
        average_spend_galactic: 0,
      });
      return;
    }

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

    // Вычисляем среднее значение
    const averageSpendGalactic = totalSpendGalactic / rowCount;

    // Формируем и отправляем результат
    return {
      total_spend_galactic: totalSpendGalactic,
      rows_affected: rowCount,
      less_spent_at: parseInt(lessSpentAt),
      big_spent_at: parseInt(bigSpentAt),
      less_spent_value: lessSpentValue,
      big_spent_value: bigSpentValue,
      average_spend_galactic: averageSpendGalactic,
      big_spent_civ: bigSpentCiv,
      less_spent_civ: lessSpentCiv,
    };
  };

  return new TransformStream({
    transform(chunk, controller) {
      if (isFirstLine) {
        isFirstLine = false;

        fillKeyMap(chunk);
      } else {
        rowCount++;

        handleRecord(chunk);

        if (rowCount % 1000 === 0) {
          const result = aggregateStats();
          controller.enqueue(JSON.stringify(result));
        }
      }
    },
    flush(controller) {
      const result = aggregateStats();

      controller.enqueue(JSON.stringify(result));
    },
  });
};
