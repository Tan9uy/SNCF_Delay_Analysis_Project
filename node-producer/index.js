import fetch from "node-fetch";
import dotenv from "dotenv";
import { KafkaClient, Producer } from "kafka-node";
import moment from "moment";
import { createTopics } from "./src/topic.js";
import readline from "readline";
import { Gauge, register } from "prom-client";
import express from "express";

const DEFAULT_INTERVAL = 30_000; // 30 secondes
const PARTITIONER_TYPE_CYCLIC = 2;
const API_DATE_FORMAT = "YYYYMMDDTHHmmss";
const PROMETHEUS_INTERVAL = 30_000; // 30 secondes

let currentObjCount = 0;

let benchmarkTask;

// Load env values
dotenv.config();
const {
  SNCF_TOKEN,
  SNCF_ENDPOINT,
  KAFKA_HOST,
  KAFKA_PORT,
  INTERVAL,
  TOPIC,
  PORT,
  BENCHMARK_ENABLE
} = process.env;

// Prometheus metric
const objectCounter = new Gauge({
  name: "object_counter_producer",
  help: `Amount of objects proceed during the last ${
    PROMETHEUS_INTERVAL / 1000
  } seconds.`,
});

// Metrics endpoint
const app = express();
app.listen(PORT);

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", register.contentType);
  res.end(await register.metrics());
});

console.log(
  "> API des métriques disponible sur le port " + PORT + " : /metrics"
);

// Init Kafka producer
const kafkaClient = new KafkaClient({
  kafkaHost: KAFKA_HOST + ":" + KAFKA_PORT,
});
kafkaClient.connect();

const kafkaProducer = new Producer(kafkaClient, {
  partitionerType: PARTITIONER_TYPE_CYCLIC,
});

// Initialise le header
const basicAuth = Buffer.from(`${SNCF_TOKEN}:`).toString("base64");

const headers = { Authorization: `Basic ${basicAuth}` };

function sendMessage() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  return new Promise((resolve) =>
    rl.question("", async (ans) => {
      rl.close();

      await fetchWithCache();

      resolve(ans);
    })
  );
}

// Kafka events
kafkaProducer.on("ready", async () => {
  console.log("> Producteur connecté à Kafka avec succès !");

  createTopics(kafkaClient);

  /* while (true) {
        await sendMessage();
    } */

  setInterval(() => {
    // update metrics
    objectCounter.set(currentObjCount);
    // reset counter
    currentObjCount = 0;
  }, PROMETHEUS_INTERVAL);


  if (BENCHMARK_ENABLE == 1) {
    console.log(">> Test de charge activé <<")
    benchmark([5_000, 4_000, 3_000, 2_000, 1_000, 750, 500, 250], 60_000)
  } else {
    setInterval(fetchApi, INTERVAL || DEFAULT_INTERVAL);
  }
});

function benchmark(steps, duration) {
  // Réduit le délais entre les fetchs
  if (benchmarkTask != null) {
    clearInterval(benchmarkTask);
  }

  if (steps.length == 0) {
    console.log("[Fin du benchmark]")
    return;
  }

  console.log("[Benchmark " + steps[0] + " ms]")
  benchmarkTask = setInterval(fetchWithCache, steps[0]);

  // Prévois la prochaine réduction de délais entre les fetchs 
  steps.shift();
  setTimeout(() => benchmark(steps, duration), duration);
}


kafkaClient.on("connect", () => {
  console.log("> Connecté à Kafka avec succès !");
});

kafkaClient.on("close", (e) => {
  console.log(e);
});

kafkaProducer.on("error", (err) => {
  console.error("Une erreur Kafka est survenue :", err);
});

let result;
async function fetchWithCache() {
  try {

    if (!result) {
      console.log("Fetch API");
      // Construit l'URL
      const since = moment()
        .subtract(1_800_000, 'milliseconds')
        .format(API_DATE_FORMAT);
      const until = moment().format(API_DATE_FORMAT);

      const params = new URLSearchParams({
        count: 10_000,
        since,
        until,
        data_freshness: "realtime",
      });

      const url = `${SNCF_ENDPOINT}?${params}`;

      // Effectue l'appel
      const response = await fetch(url, {
        method: "GET",
        headers,
      });
      const data = await response.json();

      // Envoie les résultats à Kafka
      if (data.disruptions) {
        result = data.disruptions.map((disruption) => {

          if ('status' in disruption) {
            disruption['status'] = 'past';
          }

          return {
            topic: TOPIC,
            messages: JSON.stringify(disruption),
          };
        });
      }
    } else {
      console.log("Cache API");
    }

    await splitAndSendMessage(result);
  } catch (e) {
    return;
  }
}

async function fetchApi() {
  // Construit l'URL
  const since = moment()
    .subtract(INTERVAL, "milliseconds")
    .format(API_DATE_FORMAT);
  const until = moment().format(API_DATE_FORMAT);

  const params = new URLSearchParams({
    count: 10_000,
    since,
    until,
    data_freshness: "realtime",
  });

  const url = `${SNCF_ENDPOINT}?${params}`;

  // Effectue l'appel
  try {
    const response = await fetch(url, {
      method: "GET",
      headers,
    });
    const data = await response.json();

    // Envoie les résultats à Kafka
    if (data.disruptions) {
      const payloads = data.disruptions.map((disruption) => {
        return {
          topic: TOPIC,
          messages: JSON.stringify(disruption),
        };
      });

      await splitAndSendMessage(payloads);
    }
  } catch (e) {
    return;
  }
}

async function splitAndSendMessage(payloads) {
  currentObjCount += payloads.length;

  console.log(payloads.length);

  let messageIndex = 0;
  while (messageIndex < payloads.length) {
    const nextMessageIndex = Math.min(messageIndex + 100, payloads.length);

    const smallPayloads = payloads.slice(messageIndex, nextMessageIndex);
    messageIndex = nextMessageIndex;

    kafkaProducer.send(smallPayloads, (error, res) => {
      if (error != null) {
        console.error("Erreur lors de l'envoie d'une donnée :", error);
      }
    });
  }
}
