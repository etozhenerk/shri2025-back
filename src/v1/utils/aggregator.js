import { aggregateStats } from "./statsAggregator.js";

/**
 * Запись из CSV файла
 * @typedef {Object} CsvRecord
 * @property {string} id - Идентификатор записи
 * @property {string} civ - Локальная валюта
 * @property {string} developer_id - Идентификатор разработчика
 * @property {string} date - Дата транзакции
 * @property {string} spend - Сумма расходов
 */

/**
 * Функция для получения курса обмена локальной валюты на кредиты
 * @typedef {function(string, string): number} GetExchangeRate
 * @param {string} date - Дата в формате строки
 * @param {string} civilization - Название цивилизации
 * @returns {number} Курс обмена
 */

/**
 * Преобразует текст CSV формата в массив объектов
 * @param {string} text CSV с данными
 * @returns {Array<CsvRecord>} Массив объектов с данными из CSV
 */
const transformTextToValues = (text) => {
    const [keys, ...body] = text.split("\n");
    const keyMap = keys.split(",").map((key) => key.trim());

    const result = [];

    for (const line of body) {
        const lineValues = line.split(",").map((key) => key.trim());

        // Пропускаем невалидные строки
        if (lineValues.length !== keyMap.length) {
            continue;
        }

        let isInvalid = false;

        const record = lineValues.reduce((acc, value, index) => {
            const key = keyMap[index];

            if (key === "civ") {
                if (
                    value !== "humans" &&
                    value !== "blobs" &&
                    value !== "monsters"
                ) {
                    isInvalid = true;
                }
            }

            if (key === "spend") {
                if (value < 0) {
                    isInvalid = true;
                }
            }

            acc[key] = value;

            return acc;
        }, {});

        if (isInvalid) {
            continue;
        }

        result.push(record);
    }

    return result;
};

/**
 * @param {Object} options
 * @param {GetExchangeRate} options.getExchangeRate - функция, которая возвращает курс обмена локальной валюты на кредиты
 * @returns {{
 *   addText: (chunk: string) => void,
 *   aggregate: () => Array<CsvRecord>
 * }}
 */
export const createAggregator = (options) => {
    const { getExchangeRate } = options;
    let text = "";

    return {
        addText: (chunk) => {
            text += chunk;
        },

        aggregate: () => {
            const values = transformTextToValues(text);

            const exchangeRates = values.map((value) =>
                getExchangeRate(+value.date, value.civ)
            );

            const transformedValues = values.map((value, index) => ({
                ...value,
                spend: +value.spend * exchangeRates[index],
            }));

            return aggregateStats(transformedValues);
        },
    };
};
