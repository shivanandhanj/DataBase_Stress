const { Client } = require('pg');
const promiseTimeout = require('promise-timeout');
const logger = require('../utils/logger');

const DB_CONFIG = {
  host: 'localhost',
  port: 5432,
  user: 'test_user',
  password: 'password',
  database: 'testdb',
  connectionTimeoutMillis: 10000,
};

async function run(totalConnections, timeoutMs) {
  const results = {
    database: 'postgres',
    totalConnections,
    successful: 0,
    failed: 0,
    errors: [],
    responseTimes: [],
    startMemory: process.memoryUsage().rss,
    peakMemory: 0,
    startTime: Date.now(),
  };

  const promises = [];

  for (let i = 0; i < totalConnections; i++) {
    const promise = (async (index) => {
      const startTime = process.hrtime.bigint();
      let client;

      try {
        client = new Client(DB_CONFIG);
        await promiseTimeout.timeout(client.connect(), timeoutMs);
        await client.query('SELECT 1 as value');
        await client.end();

        const endTime = process.hrtime.bigint();
        const durationMs = Number(endTime - startTime) / 1_000_000;
        results.responseTimes.push(durationMs);
        results.successful++;

      } catch (error) {
        results.failed++;
        const errorMsg = error.code || error.message;
        if (!results.errors.includes(errorMsg)) {
          results.errors.push(errorMsg);
        }
      } finally {
        if (client) await client.end().catch(() => {});
        const currentMemory = process.memoryUsage().rss;
        if (currentMemory > results.peakMemory) {
          results.peakMemory = currentMemory;
        }
      }
    })(i);

    promises.push(promise);
  }

  await Promise.allSettled(promises);
  results.endTime = Date.now();
  results.totalTime = (results.endTime - results.startTime) / 1000;
  results.avgResponseTime = results.responseTimes.length > 0 
    ? Math.round(results.responseTimes.reduce((a, b) => a + b, 0) / results.responseTimes.length)
    : 0;

  return results;
}

module.exports = { run };