import { TransformStream } from "node:stream/web";

/**
 * Создает трансформ-стрим для агрегации статистики по расходам
 * @returns {TransformStream} Трансформ-стрим для агрегации данных
 */
export const createStatsAggregator = () => {
    // Структуры для агрегации данных на лету
    const spendsByDate = {};
    const spendsByCiv = {};
    let rowCount = 0;
    let totalSpendGalactic = 0;

    const handleRecord = (record) => {
        const { spend, date, civ } = record;
        const spendValue = Number(spend);
        // Добавляем расходы в общую сумму
        totalSpendGalactic += spendValue;

        // Группируем расходы по дням
        if (!spendsByDate[date]) {
            spendsByDate[date] = 0;
        }
        spendsByDate[date] += spendValue;

        // Группируем расходы по цивилизациям
        if (!spendsByCiv[civ]) {
            spendsByCiv[civ] = 0;
        }
        spendsByCiv[civ] += spendValue;
    };

    const aggregateStats = (controller) => {
        // Находим дни с минимальными и максимальными расходами
        const spendsByDateEntries = Object.entries(spendsByDate);

        // Проверяем, что есть хотя бы одна запись
        if (spendsByDateEntries.length === 0) {
            return {
                total_spend_galactic: 0,
                rows_affected: 0,
                average_spend_galactic: 0,
            };
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
        const result = {
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

        return result;
    };

    return new TransformStream({
        transform(record) {
            rowCount++;

            handleRecord(record);
        },
        flush(controller) {
            const result = aggregateStats();

            controller.enqueue(JSON.stringify(result));
        },
    });
};
