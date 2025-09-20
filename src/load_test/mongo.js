const { MongoClient } = require('mongodb');
const promiseTimeout = require('promise-timeout');
const logger = require('../utils/logger');

const DB_CONFIG = {
  url: 'mongodb://test_user:password@localhost:27017/testdb',
  serverSelectionTimeoutMS: 10000,
  socketTimeoutMS: 10000,
};

async function run(totalConnections, timeoutMs) {
  const results = {
    database: 'mongodb',
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
        client = new MongoClient(DB_CONFIG.url, {
          ...DB_CONFIG,
          minPoolSize: 1,
          maxPoolSize: 1, // Force new connection for each test
        });

        await promiseTimeout.timeout(client.connect(), timeoutMs);
        await client.db().command({ ping: 1 });
        await client.close();

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
        if (client) await client.close().catch(() => {});
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