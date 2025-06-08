import fs from "node:fs";
import http from "node:http";
import { pipeline } from "node:stream/promises";

const passedUrlString = process.argv[2];

if (!passedUrlString) {
  console.error("URL is required");
  process.exit(1);
}

const url = new URL(passedUrlString);
const { hostname, port, pathname } = url;

const INPUT_FILE = "files/input.csv";
const OUTPUT_FILE = "files/output.json";

if (!fs.existsSync(INPUT_FILE)) {
  console.error(
    `Файл ${INPUT_FILE} не найден. Пожалуйста, сгенерируйте его с помощью generateInput`
  );
  process.exit(1);
}

if (!fs.existsSync("files")) {
  fs.mkdirSync("files");
}

function bytesToMB(bytes) {
  const mb = bytes / (1024 * 1024);
  return mb.toFixed(2);
}

const fileStats = fs.statSync(INPUT_FILE);
const fileSizeMB = bytesToMB(fileStats.size);
console.log(`Sending file ${INPUT_FILE} (${fileSizeMB} MB) to server...`);

// Функция для отправки запроса
async function sendRequest() {
  return new Promise((resolve, reject) => {
    // Создаем поток чтения из файла
    const fileStream = fs.createReadStream(INPUT_FILE);

    // Отслеживаем прогресс отправки данных
    let bytesSent = 0;
    let lastLogTime = Date.now();

    fileStream.on("data", (chunk) => {
      bytesSent += chunk.length;

      // Обновляем прогресс каждую секунду
      const now = Date.now();
      if (now - lastLogTime > 1000) {
        const percentComplete = ((bytesSent / fileStats.size) * 100).toFixed(2);
        const mbSent = bytesToMB(bytesSent);
        process.stdout.write(`\rSent: ${mbSent} MB (${percentComplete}%)`);
        lastLogTime = now;
      }
    });

    console.log(`Sending request to ${url.href}`);

    const startTime = process.hrtime.bigint();

    const requestOptions = {
      hostname,
      port,
      path: pathname,
      method: "POST",
      headers: {
        "Content-Type": "text/csv",
        "Content-Length": fileStats.size,
      },
    };

    const req = http.request(requestOptions, (res) => {
      console.log(`\nResponse status: ${res.statusCode} ${res.statusMessage}`);

      // Создаем поток записи для сохранения ответа
      const outputStream = fs.createWriteStream(OUTPUT_FILE);

      // Обрабатываем ответ
      let bytesReceived = 0;
      res.on("data", (chunk) => {
        bytesReceived += chunk.length;
        const mbReceived = bytesToMB(bytesReceived);
        process.stdout.write(`\rReceived: ${mbReceived} MB`);
      });

      // Используем pipeline для обработки ответа
      pipeline(res, outputStream)
        .then(() => {
          const endTime = process.hrtime.bigint();
          const duration = Number(endTime - startTime) / 1_000_000;

          console.log(
            `\nTime taken to execute request: ${(duration / 1000).toFixed(
              2
            )} seconds`
          );
          console.log(`Response saved to file ${OUTPUT_FILE}`);

          // Проверяем количество строк в ответе
          const responseStats = fs.statSync(OUTPUT_FILE);
          const mbReceived = bytesToMB(responseStats.size);
          console.log(`Size of response: ${mbReceived} MB`);

          // Подсчет строк в файле (без загрузки его целиком в память)
          countLinesInFile(OUTPUT_FILE)
            .then((lineCount) => {
              console.log(`Number of lines in response: ${lineCount}`);
              resolve();
            })
            .catch(reject);
        })
        .catch(reject);
    });

    // Обработка ошибок запроса
    req.on("error", (error) => {
      console.error(`Error sending request: ${error.message}`);
      reject(error);
    });

    // Отправляем данные через поток
    fileStream.pipe(req);
  });
}

// Функция для подсчета строк в файле
async function countLinesInFile(filePath) {
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(filePath);
    let lineCount = 0;
    let buffer = "";

    fileStream.on("data", (chunk) => {
      const content = buffer + chunk.toString();
      const lines = content.split("\n");
      buffer = lines.pop() || "";
      lineCount += lines.length;
    });

    fileStream.on("end", () => {
      if (buffer.length > 0) {
        lineCount++;
      }
      resolve(lineCount);
    });

    fileStream.on("error", reject);
  });
}

// Запускаем отправку запроса
console.log("Запуск отправки запроса...");
sendRequest()
  .then(() => {
    console.log("Запрос успешно завершен");
  })
  .catch((error) => {
    console.error("Ошибка при выполнении скрипта:", error);
    process.exit(1);
  });
