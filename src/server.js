import http from "http";
import { aggregateV1 } from "./v1/aggregate.js";
import { aggregateV2 } from "./v2/aggregate.js";
import { aggregateV2asStream } from "./v2/aggregate_as_stream.js";
import { aggregateV3 } from "./v3/aggregate.js";
import { aggregateV3asStream } from "./v3/aggregate_as_stream.js";
import { aggregateV4 } from "./v4/aggregate.js";

// Простой логгер использования памяти
// Уже скоро ты будешь использовать более продвинутые инструменты для этого
function logMemoryUsage() {
    const bytesToMB = (bytes) => Math.round(bytes / 1024 / 1024);

    const memoryUsage = process.memoryUsage();

    console.clear();
    console.log("Memory Usage:");
    console.log(`RSS: ${bytesToMB(memoryUsage.rss)}MB`);
    console.log(`Heap Total: ${bytesToMB(memoryUsage.heapTotal)}MB`);
    console.log(`Heap Used: ${bytesToMB(memoryUsage.heapUsed)}MB`);
    console.log(`External: ${bytesToMB(memoryUsage.external)}MB`);
    console.log("------------------------");
}

const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    // Обработка preflight запросов
    if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
    }

    if (req.url === "/v1/stats:aggregate") {
        return aggregateV1(req, res);
    }

    // Предполагается, что студенты напишут такой вариант
    if (req.url === "/v2/stats:aggregate") {
        return aggregateV2(req, res);
    }

    if (req.url === "/v2/stats:aggregate_as_stream") {
        return aggregateV2asStream(req, res);
    }

    // Возможно кто-то захочет парсить буферы самостоятельно и напишет что-то подобное
    if (req.url === "/v3/stats:aggregate") {
        return aggregateV3(req, res);
    }

    if (req.url === "/v3/stats:aggregate_as_stream") {
        return aggregateV3asStream(req, res);
    }

    if (req.url === "/v4/stats:aggregate") {
        return aggregateV4(req, res);
    }

    res.writeHead(404, "Not found");
    res.end();
});

server.listen(3000, () => {
    console.log("Server is running on port 3000");
    setInterval(logMemoryUsage, 100);
});
