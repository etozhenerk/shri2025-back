import { TransformStream } from "node:stream/web";
import { setTimeout } from "node:timers/promises";

const NEW_LINE_CHAR_CODE = 10; // "\n"
const COMMA_DELIMITER_CODE = 44; // ","

// Предварительно созданные буферы для часто используемых строк
const SPEND_KEY = Buffer.from("spend");
const DATE_KEY = Buffer.from("date");
const CIV_KEY = Buffer.from("civ");

const HUMAN_KEY = Buffer.from("humans");
const BLOBS_KEY = Buffer.from("blobs");
const MONSTERS_KEY = Buffer.from("monsters");

// Вспомогательная функция для сравнения буферов
const compareBuffers = (buf1, offset1, length1, buf2) => {
    if (length1 !== buf2.length) return false;
    for (let i = 0; i < length1; i++) {
        if (buf1[offset1 + i] !== buf2[i]) return false;
    }
    return true;
};

// Функция для преобразования части буфера в число без создания строки
const parseNumberFromBuffer = (buffer, start, end) => {
    let result = 0;
    let negative = false;

    for (let i = start; i < end; i++) {
        const char = buffer[i];
        if (char === 45) {
            // "-"
            negative = true;
            continue;
        }
        if (char >= 48 && char <= 57) {
            // 0-9
            result = result * 10 + (char - 48);
        }
    }

    return negative ? -result : result;
};

// Функция для быстрого хеширования буфера для использования в качестве ключа
const hashBuffer = (buffer, start, end) => {
    let hash = 0;
    for (let i = start; i < end; i++) {
        hash = (hash << 5) - hash + buffer[i];
    }
    return hash >>> 0; // Приводим к беззнаковому 32-битному целому
};

const exchangeRate = new Map();
exchangeRate.set(hashBuffer(HUMAN_KEY, 0, HUMAN_KEY.length), 0.5);
exchangeRate.set(hashBuffer(BLOBS_KEY, 0, BLOBS_KEY.length), 1);
exchangeRate.set(hashBuffer(MONSTERS_KEY, 0, MONSTERS_KEY.length), 1.5);

const getExchangeRate = (civilizationHash) => {
    return exchangeRate.get(civilizationHash);
};

/**
 * @param {Object} params
 * @param {number} params.rows
 * @returns {TransformStream}
 */
export const aggregate = (params) => {
    const { rows } = params;
    // Выделяем буфер один раз с запасом
    const buffer = Buffer.alloc(256000);
    let bufferOffset = 0;

    let isFirstLine = true;
    let spendIndex = -1;
    let dateIndex = -1;
    let civIndex = -1;

    // Используем Map с числовыми ключами вместо объектов со строковыми ключами
    const spendsByDate = new Map();
    const spendsByCiv = new Map();

    let rowCount = 0;
    let totalSpendGalactic = 0;

    // Буферы для хранения текущих значений
    // (мы будем хранить только хеши и числа, а не строки)
    let currentSpend = 0;
    let currentDateHash = 0;
    let currentCivHash = 0;

    // Карта, связывающая хеши с оригинальными значениями для финального отчета
    const dateHashToString = new Map();
    const civHashToString = new Map();
    civHashToString.set(hashBuffer(HUMAN_KEY, 0, HUMAN_KEY.length), "humans");
    civHashToString.set(hashBuffer(BLOBS_KEY, 0, BLOBS_KEY.length), "blobs");
    civHashToString.set(
        hashBuffer(MONSTERS_KEY, 0, MONSTERS_KEY.length),
        "monsters"
    );

    // Обработка одной строки из CSV
    const processLine = (lineStart, lineEnd) => {
        if (isFirstLine) {
            isFirstLine = false;

            // Обработка заголовка: определяем индексы столбцов
            let colIndex = 0;
            let fieldStart = lineStart;

            for (let i = lineStart; i < lineEnd; i++) {
                if (buffer[i] === COMMA_DELIMITER_CODE) {
                    // Проверяем, какой это столбец
                    if (
                        compareBuffers(
                            buffer,
                            fieldStart,
                            i - fieldStart,
                            SPEND_KEY
                        )
                    ) {
                        spendIndex = colIndex;
                    } else if (
                        compareBuffers(
                            buffer,
                            fieldStart,
                            i - fieldStart,
                            DATE_KEY
                        )
                    ) {
                        dateIndex = colIndex;
                    } else if (
                        compareBuffers(
                            buffer,
                            fieldStart,
                            i - fieldStart,
                            CIV_KEY
                        )
                    ) {
                        civIndex = colIndex;
                    }

                    colIndex++;
                    fieldStart = i + 1;
                }
            }

            // Проверяем последнее поле
            if (
                compareBuffers(
                    buffer,
                    fieldStart,
                    lineEnd - fieldStart,
                    SPEND_KEY
                )
            ) {
                spendIndex = colIndex;
            } else if (
                compareBuffers(
                    buffer,
                    fieldStart,
                    lineEnd - fieldStart,
                    DATE_KEY
                )
            ) {
                dateIndex = colIndex;
            } else if (
                compareBuffers(
                    buffer,
                    fieldStart,
                    lineEnd - fieldStart,
                    CIV_KEY
                )
            ) {
                civIndex = colIndex;
            }

            return;
        }

        // Обрабатываем данные строки
        rowCount++;

        let colIndex = 0;
        let fieldStart = lineStart;

        // Сбрасываем текущие значения
        currentSpend = 0;
        currentDateHash = 0;
        currentCivHash = 0;

        let isInvalidLine = false;

        // Проходим по всем полям строки
        for (let i = lineStart; i <= lineEnd; i++) {
            if (i === lineEnd || buffer[i] === COMMA_DELIMITER_CODE) {
                // Если строка не содержит ни одной запятой, то она невалидна
                if (i === lineEnd && colIndex === 0) {
                    isInvalidLine = true;
                    break;
                }

                // Обрабатываем поле в зависимости от его индекса
                if (colIndex === spendIndex) {
                    currentSpend = parseNumberFromBuffer(buffer, fieldStart, i);
                    if (currentSpend < 0) {
                        isInvalidLine = true;
                        break;
                    }
                } else if (colIndex === dateIndex) {
                    currentDateHash = hashBuffer(buffer, fieldStart, i);

                    // Сохраняем оригинальное значение даты, только если его еще нет
                    if (!dateHashToString.has(currentDateHash)) {
                        // Здесь придется создать строку, но делаем это только один раз для каждой уникальной даты
                        const dateStr = buffer.toString("utf8", fieldStart, i);
                        dateHashToString.set(currentDateHash, dateStr);
                    }
                } else if (colIndex === civIndex) {
                    currentCivHash = hashBuffer(buffer, fieldStart, i);

                    // Сохраняем оригинальное значение цивилизации
                    if (!civHashToString.has(currentCivHash)) {
                        isInvalidLine = true;
                        break;
                    }
                }

                colIndex++;
                fieldStart = i + 1;

                if (i === lineEnd) break;
            }
        }

        if (isInvalidLine) {
            rowCount--;
            return;
        }

        currentSpend = currentSpend * getExchangeRate(currentCivHash);

        // Обновляем статистику
        totalSpendGalactic += currentSpend;

        // Обновляем расходы по дате
        let dateSpend = spendsByDate.get(currentDateHash) || 0;
        spendsByDate.set(currentDateHash, dateSpend + currentSpend);

        // Обновляем расходы по цивилизации
        let civSpend = spendsByCiv.get(currentCivHash) || 0;
        spendsByCiv.set(currentCivHash, civSpend + currentSpend);
    };

    // Подготовка итоговой статистики
    const aggregateStats = () => {
        if (rowCount === 0) {
            return {
                total_spend_galactic: 0,
                rows_affected: 0,
                average_spend_galactic: 0,
            };
        }

        // Находим даты с минимальными и максимальными расходами
        let minDateHash = null;
        let maxDateHash = null;
        let minDateSpend = Infinity;
        let maxDateSpend = -Infinity;

        for (const [dateHash, spend] of spendsByDate.entries()) {
            if (spend < minDateSpend) {
                minDateSpend = spend;
                minDateHash = dateHash;
            }
            if (spend > maxDateSpend) {
                maxDateSpend = spend;
                maxDateHash = dateHash;
            }
        }

        // Находим цивилизации с минимальными и максимальными расходами
        let minCivHash = null;
        let maxCivHash = null;
        let minCivSpend = Infinity;
        let maxCivSpend = -Infinity;

        for (const [civHash, spend] of spendsByCiv.entries()) {
            if (spend < minCivSpend) {
                minCivSpend = spend;
                minCivHash = civHash;
            }
            if (spend > maxCivSpend) {
                maxCivSpend = spend;
                maxCivHash = civHash;
            }
        }

        // Получаем строки из хешей
        const lessSpentAt = dateHashToString.get(minDateHash);
        const bigSpentAt = dateHashToString.get(maxDateHash);
        const lessSpentCiv = civHashToString.get(minCivHash);
        const bigSpentCiv = civHashToString.get(maxCivHash);

        // Вычисляем среднее значение
        const averageSpendGalactic = totalSpendGalactic / rowCount;

        return {
            total_spend_galactic: totalSpendGalactic,
            rows_affected: rowCount,
            less_spent_at: parseInt(lessSpentAt),
            big_spent_at: parseInt(bigSpentAt),
            less_spent_value: minDateSpend,
            big_spent_value: maxDateSpend,
            average_spend_galactic: averageSpendGalactic,
            big_spent_civ: bigSpentCiv,
            less_spent_civ: lessSpentCiv,
        };
    };

    return new TransformStream({
        async transform(chunk, controller) {
            // Если буфер может переполниться, увеличиваем его размер
            if (bufferOffset + chunk.length > buffer.length) {
                const newBuffer = Buffer.alloc(buffer.length * 2);
                buffer.copy(newBuffer, 0, 0, bufferOffset);
                buffer = newBuffer;
            }

            // Копируем новые данные в буфер
            chunk.copy(buffer, bufferOffset);
            const newDataEnd = bufferOffset + chunk.length;

            // Обрабатываем все полные строки
            let lineStart = 0;
            for (let i = 0; i < newDataEnd; i++) {
                if (buffer[i] === NEW_LINE_CHAR_CODE) {
                    processLine(lineStart, i);
                    lineStart = i + 1;

                    if (rowCount % rows === 0) {
                        const result = aggregateStats();

                        await setTimeout(100);
                        controller.enqueue(result);
                    }
                }
            }

            // Если остались данные, которые не заканчиваются новой строкой,
            // переносим их в начало буфера
            if (lineStart < newDataEnd) {
                const remainingLength = newDataEnd - lineStart;
                buffer.copy(buffer, 0, lineStart, newDataEnd);
                bufferOffset = remainingLength;
            } else {
                bufferOffset = 0;
            }
        },
        flush(controller) {
            if (bufferOffset > 0) {
                processLine(0, bufferOffset);
            }

            const result = aggregateStats();

            return setTimeout(100).then(() => {
                controller.enqueue(result);
            });
        },
    });
};
