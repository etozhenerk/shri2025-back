const MAX_DEVELOPER_ID = 10_000_000_000_000;
const CIVILIZATIONS = ["humans", "blobs", "monsters"];
const ERROR_CHANCE = 0.01; // 1% шанс ошибки для каждого типа

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function generateRandomString(length) {
    const chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+-=[]{}|;:<>?";
    let result = "";
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

function shouldGenerateError() {
    return Math.random() < ERROR_CHANCE;
}

export function createRecordGenerator() {
    let recordId = 1;

    const generateRecord = () => {
        // Генерируем полностью невалидную строку
        if (shouldGenerateError()) {
            const length = getRandomInt(16, 32);
            return generateRandomString(length);
        }

        const civ = CIVILIZATIONS[getRandomInt(0, CIVILIZATIONS.length - 1)];
        const developer_id = getRandomInt(0, MAX_DEVELOPER_ID);
        const date = getRandomInt(0, 364);

        // Генерируем отрицательный spend
        let spend = getRandomInt(0, 1000);
        if (shouldGenerateError()) {
            spend = -getRandomInt(1, 1000);
        }

        // Генерируем неизвестную цивилизацию
        let finalCiv = civ;
        if (shouldGenerateError()) {
            finalCiv = "unknown_" + generateRandomString(5);
        }

        recordId++;

        return [recordId, finalCiv, developer_id, date, spend].join(",");
    };

    const generateRecordsBlock = (size) => {
        let data = "";

        for (let i = 0; i < size; i++) {
            data += generateRecord() + "\n";
        }

        return data;
    };

    const getTotalRecords = () => recordId;

    return {
        generateRecord,
        generateRecordsBlock,
        getTotalRecords,
    };
}
