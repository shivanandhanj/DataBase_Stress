const { Client: PgClient } = require('pg');
const mysql = require('mysql2/promise');
const { MongoClient } = require('mongodb');
const { table } = require('table');
const chalk = require('chalk');
const promiseTimeout = require('promise-timeout');

// ===== CONFIGURATION =====
const TOTAL_CONNECTIONS = 500;
const CONNECTION_TIMEOUT_MS = 10000; // 10 seconds
const TEST_QUERY = {
  postgres: 'SELECT 1 as value',
  mysql: 'SELECT 1 as value',
  mongodb: { ping: 1 } // MongoDB's ping command
};

const DB_CONFIG = {
  postgres: {
    host: 'localhost',
    port: 5432,
    user: PROCCESS.env.pgname,
    password: PROCCESS.env.ppassword,
    database: PROCCESS.env.postgresdb,
    // This is crucial for the test. We don't want the pool, just a raw connection.
    connectionTimeoutMillis: CONNECTION_TIMEOUT_MS,
  },
  mysql: {
    host: 'localhost',
    port: 3306,
    user: PROCCESS.env.mysqlname,
    password: PROCCESS.env.mpassword,
    database: PROCCESS,
    connectTimeout: CONNECTION_TIMEOUT_MS,
    // Enable for more verbose MySQL errors
    // debug: true
  },
  mongodb: {
    url: PROCCESS.env.mongodburl || 'mongodb://localhost:27017/testdb',
    // Critical: This defines the connection pool for a single MongoClient!
    // We are testing the DB's maxIncomingConnections, not the driver's pool.
    poolSize: 1, // Set to 1 to ensure each test creates a new connection pool (TCP socket)
    serverSelectionTimeoutMS: CONNECTION_TIMEOUT_MS,
    socketTimeoutMS: CONNECTION_TIMEOUT_MS,
    // Newer option to replace useUnifiedTopology
    monitorCommands: true
  }
};

// ===== TEST FUNCTION =====
async function testDatabase(dbName) {
  console.log(chalk.yellow(`\n--- Starting ${dbName.toUpperCase()} Test ---`));

  const results = {
    successful: 0,
    failed: 0,
    errors: [],
    responseTimes: [],
    startMemory: process.memoryUsage().rss,
    peakMemory: 0,
    startTime: Date.now(),
    recoveryTime: null
  };

  const promises = [];
  const testConfig = DB_CONFIG[dbName];

  // Create an array of promises for each connection attempt
  for (let i = 0; i < TOTAL_CONNECTIONS; i++) {
    const promise = (async (index) => {
      const startTime = process.hrtime.bigint();
      let client;

      try {
        switch (dbName) {
          case 'postgres':
            client = new PgClient(testConfig);
            await client.connect();
            await client.query(TEST_QUERY.postgres);
            await client.end();
            break;

          case 'mysql':
            client = await mysql.createConnection(testConfig);
            await client.execute(TEST_QUERY.mysql);
            await client.end();
            break;

          case 'mongodb':
            // For MongoDB, we test the server's connection limit by creating new MongoClient instances.
            // Each one will establish a new TCP connection.
            client = new MongoClient(testConfig.url, testConfig);
            await client.connect();
            await client.db().command(TEST_QUERY.mongodb);
            await client.close();
            break;
        }

        const endTime = process.hrtime.bigint();
        const durationMs = Number(endTime - startTime) / 1_000_000; // Convert nanoseconds to milliseconds
        results.responseTimes.push(durationMs);
        results.successful++;

      } catch (error) {
        results.failed++;
        // Store unique error messages
        const errorMsg = error.code || error.message;
        if (!results.errors.includes(errorMsg)) {
          results.errors.push(errorMsg);
        }
      } finally {
        // Update peak memory usage
        const currentMemory = process.memoryUsage().rss;
        if (currentMemory > results.peakMemory) {
          results.peakMemory = currentMemory;
        }
      }
    })(i).catch(e => console.error('Unhandled promise rejection in test:', e));

    // Wrap the individual connection promise in a timeout
    promises.push(promiseTimeout.timeout(promise, CONNECTION_TIMEOUT_MS).catch(error => {
      if (error instanceof promiseTimeout.TimeoutError) {
        results.failed++;
        if (!results.errors.includes('Connection Timeout')) {
          results.errors.push('Connection Timeout');
        }
      }
    }));
  }

  // Wait for all connection attempts to settle
  await Promise.allSettled(promises);
  results.endTime = Date.now();
  results.totalTime = (results.endTime - results.startTime) / 1000;

  // Calculate average response time for successful connections
  const avgTime = results.responseTimes.length > 0
    ? (results.responseTimes.reduce((sum, time) => sum + time, 0) / results.responseTimes.length).toFixed(0)
    : 0;

  // Generate the visual connection pattern
  const successBlocks = Math.round((results.successful / TOTAL_CONNECTIONS) * 30);
  const failBlocks = 30 - successBlocks;
  const connectionPattern = `0-${results.successful}:   [${'█'.repeat(successBlocks)}] Success\n` +
                            `${results.successful+1}-${TOTAL_CONNECTIONS}: [${'×'.repeat(failBlocks)}] ${results.errors[0] || 'Failed'}`;

  // Format the results in the style you requested
  const resultTable = [
    [`${dbName.toUpperCase()} ${/[\d.]+/.exec(DB_CONFIG[dbName].url || '')?.[0] || 'X.Y.Z'} Results`, ''], // Dynamically get version if possible
    ['Successful connections:', `${results.successful}/${TOTAL_CONNECTIONS}`],
    ['Failed connections:', `${results.failed}`],
    ['Avg response time:', `${avgTime}ms`],
    ['Peak memory usage:', `${(results.peakMemory / 1024 / 1024).toFixed(1)}GB`],
    ['Total test time:', `${results.totalTime.toFixed(1)}s`],
    ['Primary Error:', results.errors[0] || 'None'],
    ['Connection Pattern:', connectionPattern]
  ];

  console.log(table(resultTable));
  return results;
}

// ===== MAIN RUNNER =====
async function runAllTests() {
  console.log(chalk.red.bold('=== Database Connection Pool Apocalypse Simulation ==='));
  console.log(chalk.gray(`Testing with ${TOTAL_CONNECTIONS} concurrent connections...\n`));

  // Run tests sequentially to avoid overloading your laptop
  await testDatabase('postgres');
  await testDatabase('mysql');
  await testDatabase('mongodb');

  console.log(chalk.green.bold('\n=== All Tests Completed ==='));
}

runAllTests().catch(console.error);