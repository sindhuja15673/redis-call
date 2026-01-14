
// const express = require('express');
// const http = require('http');
// const cors = require('cors');
// const bodyParser = require('body-parser');
// const { Server } = require('socket.io');
// const Redis = require('ioredis');
// const { v4: uuidv4 } = require('uuid');
// const GLOBAL_COMPLETED = `global:completed:calls`;
// const GLOBAL_RATE_KEY = (minute) => `global:rate:${minute}`;

// const PORT = process.env.PORT || 3000;
// const INSTANCE_ID = `session-${PORT}`;

// /* ---------------- DYNAMIC CONFIG ---------------- */
// const CONFIG = {
//   RATE_PER_MIN: Number(process.env.RATE_PER_MIN || 10),
//   MAX_ACTIVE: Number(process.env.MAX_ACTIVE || 5),
// };

// /* ---------------- REDIS ---------------- */
// const redis = new Redis({ host: '127.0.0.1', port: 6379 });

// /* ---------------- REDIS NAMESPACE (PER PORT) ---------------- */
// const NS = `queue:${INSTANCE_ID}`;

// const PENDING_QUEUE = `${NS}:pending`;
// const PROCESSING_LIST = `${NS}:processing`;
// const COMPLETED_LIST = `${NS}:completed`;
// const ACTIVE_CALLS = `${NS}:active`;

// /* ---------------- APP ---------------- */
// const app = express();
// const server = http.createServer(app);
// const io = new Server(server, { cors: { origin: '*' } });

// app.use(cors());
// app.use(bodyParser.json({ limit: '30mb' }));
// app.use(express.static(__dirname));

// /* ---------------- TIME KEY ---------------- */
// function minuteKey() {
//   const d = new Date();
//   return (
//     d.getUTCFullYear() +
//     String(d.getUTCMonth() + 1).padStart(2, '0') +
//     String(d.getUTCDate()).padStart(2, '0') +
//     String(d.getUTCHours()).padStart(2, '0') +
//     String(d.getUTCMinutes()).padStart(2, '0')
//   );
// }


// async function getStats() {
//   const minute = minuteKey();
//   // const RATE_KEY = `${NS}:rate:${minute}`;
//   const RATE_KEY = GLOBAL_RATE_KEY(minute);


//   return {
//     port: PORT,
//     pending: await redis.llen(PENDING_QUEUE),
//     processing: await redis.llen(PROCESSING_LIST),
//     completed: await redis.llen(COMPLETED_LIST),
//     active: Number(await redis.get(ACTIVE_CALLS)) || 0,
//     rateLimit: CONFIG.RATE_PER_MIN,
//     // usedThisMin: Number(await redis.get(RATE_KEY)) || 0,
//     usedThisMin: Number(await redis.get(GLOBAL_RATE_KEY(minute))) || 0,

//     maxActive: CONFIG.MAX_ACTIVE
//   };
// }


// /* ---------------- LUA (PER PORT LIMITS) ---------------- */
// // const luaPop = `
// // local active = tonumber(redis.call('GET', KEYS[1]) or '0')
// // if active >= tonumber(ARGV[1]) then return nil end

// // local rate = tonumber(redis.call('GET', KEYS[2]) or '0')
// // if rate >= tonumber(ARGV[2]) then return nil end

// // local job = redis.call('LPOP', KEYS[3])
// // if not job then return nil end

// // redis.call('INCR', KEYS[1])
// // redis.call('INCR', KEYS[2])
// // redis.call('EXPIRE', KEYS[2], 60)
// // redis.call('RPUSH', KEYS[4], job)

// // return job
// // `;
// const luaPop = `
// local active = tonumber(redis.call('GET', KEYS[1]) or '0')
// if active >= tonumber(ARGV[1]) then return nil end

// local globalRate = tonumber(redis.call('GET', KEYS[2]) or '0')
// if globalRate >= tonumber(ARGV[2]) then return nil end

// local job = redis.call('LPOP', KEYS[3])
// if not job then return nil end

// redis.call('INCR', KEYS[1])
// redis.call('INCR', KEYS[2])
// redis.call('EXPIRE', KEYS[2], 60)
// redis.call('RPUSH', KEYS[4], job)

// return job
// `;

// /* ---------------- UPLOAD ---------------- */
// app.post('/upload', async (req, res) => {
//   const { records } = req.body;
//   if (!Array.isArray(records)) {
//     return res.status(400).json({ error: 'records must be array' });
//   }

//   const pipe = redis.pipeline();

//   for (const r of records) {
//     const id = uuidv4();
//     pipe.hmset(`job:${id}`, {
//       requestId: id,
//       payload: JSON.stringify(r),
//       session: INSTANCE_ID,
//       createdAt: Date.now()
//     });
//     pipe.rpush(PENDING_QUEUE, id);
//   }

//   await pipe.exec();
//   res.json({ uploaded: records.length, port: PORT });
// });

// /* ---------------- STATS ---------------- */
// app.get('/stats', async (_, res) => {
//   res.json(await getStats());
// });


// /* ---------------- RESET (SAFE) ---------------- */
// // app.post('/reset', async (_, res) => {
// //   const keys = await redis.keys(`${NS}:*`);
// //   if (keys.length) await redis.del(keys);

  
// //   const freshStats = await getStats();
// //   io.emit('stats', freshStats);  

// //   res.json({ message: `Reset done for ${INSTANCE_ID}` });
// // });
// app.post('/reset', async (_, res) => {
//   const portKeys = await redis.keys(`${NS}:*`);
//   if (portKeys.length) await redis.del(portKeys);

//   const globalKeys = await redis.keys(`global:rate:*`);
//   if (globalKeys.length) await redis.del(globalKeys);

//   io.emit('stats', await getStats());
//   res.json({ message: 'System reset (global + port)' });
// });

// /* ---------------- EXPORT ---------------- */
// app.get('/export-completed', async (_, res) => {
//   const rows = await redis.lrange(COMPLETED_LIST, 0, -1);
//   res.json(rows.map(r => JSON.parse(r)));
// });


// app.get('/export-all-completed', async (_, res) => {
//   try {
//     const rows = await redis.lrange(GLOBAL_COMPLETED, 0, -1);
//     res.json(rows.map(r => JSON.parse(r)));
//   } catch (err) {
//     res.status(500).json({ error: 'Failed to fetch global completed calls' });
//   }
// });

// /* ---------------- PROCESSOR ---------------- */
// async function tryProcess() {
//   const minute = minuteKey();
//   // const RATE_KEY = `${NS}:rate:${minute}`;
//   const RATE_KEY = GLOBAL_RATE_KEY(minute);

//   const jobId = await redis.eval(
//     luaPop,
//     4,
//     ACTIVE_CALLS,
//     RATE_KEY,
//     PENDING_QUEUE,
//     PROCESSING_LIST,
//     CONFIG.MAX_ACTIVE,
//     CONFIG.RATE_PER_MIN
//   );

//   if (!jobId) return;

//   await new Promise(r => setTimeout(r, 1500));

//   const job = await redis.hgetall(`job:${jobId}`);

//   await redis.multi()
//     .lrem(PROCESSING_LIST, 1, jobId)
//     .decr(ACTIVE_CALLS)
//     .rpush(COMPLETED_LIST, JSON.stringify(job))
//      .rpush(GLOBAL_COMPLETED, JSON.stringify({
//     ...job,
//     port: PORT,
//     completedAt: Date.now()
//   }))   
//     .exec();

//   io.emit('stats', await fetchStats());
// }

// // async function fetchStats() {
// //   return (await fetch(`http://localhost:${PORT}/stats`)).json();
// // }
// async function fetchStats() {
//   return await getStats();
// }

// setInterval(tryProcess, 1000);

// /* ---------------- SOCKET ---------------- */
// io.on('connection', socket => {
//   fetchStats().then(s => socket.emit('stats', s));
// });

// /* ---------------- START ---------------- */
// server.listen(PORT, () => {
//   console.log(
//     `🚀 Port ${PORT} | Rate ${CONFIG.RATE_PER_MIN}/min | MaxActive ${CONFIG.MAX_ACTIVE}`
//   );
// });

const express = require('express');
const http = require('http');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

/* ---------------- CONFIG ---------------- */
const PORT = process.env.PORT || 3000;
const INSTANCE_ID = `session-${PORT}`;

const CONFIG = {
  RATE_PER_MIN: Number(process.env.RATE_PER_MIN || 10),
  MAX_ACTIVE: Number(process.env.MAX_ACTIVE || 5),
};

/* ---------------- REDIS ---------------- */
const redis = new Redis({ host: '127.0.0.1', port: 6379 });

/* ---------------- HELPERS ---------------- */
function minuteKey() {
  const d = new Date();
  return (
    d.getUTCFullYear() +
    String(d.getUTCMonth() + 1).padStart(2, '0') +
    String(d.getUTCDate()).padStart(2, '0') +
    String(d.getUTCHours()).padStart(2, '0') +
    String(d.getUTCMinutes()).padStart(2, '0')
  );
}

function getUserKeys(user) {
  const NS = `queue:${INSTANCE_ID}:${user}`;
  return {
    PENDING_QUEUE: `${NS}:pending`,
    PROCESSING_LIST: `${NS}:processing`,
    COMPLETED_LIST: `${NS}:completed`,
    ACTIVE_CALLS: `${NS}:active`,
    RATE_KEY: `${NS}:rate:${minuteKey()}`,
  };
}

/* ---------------- LUA SCRIPT ---------------- */
const luaPop = `
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
if active >= tonumber(ARGV[1]) then return nil end

local rate = tonumber(redis.call('GET', KEYS[2]) or '0')
if rate >= tonumber(ARGV[2]) then return nil end

local job = redis.call('LPOP', KEYS[3])
if not job then return nil end

redis.call('INCR', KEYS[1])
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], 60)
redis.call('RPUSH', KEYS[4], job)

return job
`;

/* ---------------- APP ---------------- */
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(bodyParser.json({ limit: '30mb' }));
app.use(express.static(__dirname));

/* ---------------- UPLOAD ---------------- */
app.post('/upload', async (req, res) => {
  const user = req.query.user || 'anonymous';
  const { records } = req.body;

  if (!Array.isArray(records)) {
    return res.status(400).json({ error: 'records must be array' });
  }

  const { PENDING_QUEUE } = getUserKeys(user);
  const pipe = redis.pipeline();

  for (const r of records) {
    const id = uuidv4();
    pipe.hmset(`job:${id}`, {
      requestId: id,
      payload: JSON.stringify(r),
      user,
      createdAt: Date.now(),
    });
    pipe.rpush(PENDING_QUEUE, id);
  }

  await pipe.exec();
  res.json({ uploaded: records.length, user, port: PORT });
});

/* ---------------- STATS ---------------- */
async function getStats(user) {
  const { PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, ACTIVE_CALLS } = getUserKeys(user);
  const minute = minuteKey();
  const RATE_KEY = `queue:${INSTANCE_ID}:${user}:rate:${minute}`;

  return {
    pending: await redis.llen(PENDING_QUEUE),
    processing: await redis.llen(PROCESSING_LIST),
    completed: await redis.llen(COMPLETED_LIST),
    active: Number(await redis.get(ACTIVE_CALLS)) || 0,
    rateLimit: CONFIG.RATE_PER_MIN,
    usedThisMin: Number(await redis.get(RATE_KEY)) || 0,
    maxActive: CONFIG.MAX_ACTIVE,
  };
}

/* ---------------- EXPORT ---------------- */
app.get('/export-completed', async (req, res) => {
  const user = req.query.user || 'anonymous';
  const { COMPLETED_LIST } = getUserKeys(user);

  const rows = await redis.lrange(COMPLETED_LIST, 0, -1);
  res.json(rows.map(r => JSON.parse(r)));
});

app.get('/export-all-completed', async (req, res) => {
  try {
    const rows = await redis.lrange(`global:completed:calls`, 0, -1);
    res.json(rows.map(r => JSON.parse(r)));
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch global completed calls' });
  }
});

// Get global stats (sum of completed calls across all sessions/users)
app.get("/global-stats", async (req, res) => {
  try {
    // Find all user namespaces
    const keys = await redis.keys(`queue:${INSTANCE_ID}:*:completed`);
    let totalGlobalCompleted = 0;

    for (const key of keys) {
      const len = await redis.llen(key);
      totalGlobalCompleted += len;
    }

    res.json({ totalGlobalCompleted });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to fetch global stats" });
  }
});

/* ---------------- RESET ---------------- */
app.post('/reset', async (req, res) => {
  const user = req.query.user || 'anonymous';
  const NS = `queue:${INSTANCE_ID}:${user}`;
  const keys = await redis.keys(`${NS}:*`);
  if (keys.length) await redis.del(keys);

  const stats = await getStats(user);
  io.to(user).emit('stats', stats);

  res.json({ message: `Reset done for ${user}` });
});

/* ---------------- PROCESS QUEUE ---------------- */
async function tryProcess() {
  const minute = minuteKey();
  const queues = await redis.keys(`queue:${INSTANCE_ID}:*:pending`);

  for (const pendingQueue of queues) {
    const user = pendingQueue.split(':')[2];
    const { PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, ACTIVE_CALLS } = getUserKeys(user);
    const RATE_KEY = `queue:${INSTANCE_ID}:${user}:rate:${minute}`;

    const jobId = await redis.eval(
      luaPop,
      4,
      ACTIVE_CALLS,
      RATE_KEY,
      PENDING_QUEUE,
      PROCESSING_LIST,
      CONFIG.MAX_ACTIVE,
      CONFIG.RATE_PER_MIN
    );

    if (!jobId) continue;

    await new Promise(r => setTimeout(r, 1500));
    const job = await redis.hgetall(`job:${jobId}`);

    await redis.multi()
      .lrem(PROCESSING_LIST, 1, jobId)
      .decr(ACTIVE_CALLS)
      .rpush(COMPLETED_LIST, JSON.stringify(job))
      .rpush(`global:completed:calls`, JSON.stringify({ ...job, port: PORT, completedAt: Date.now() }))
      .exec();

    const stats = await getStats(user);
    io.to(user).emit('stats', stats);
  }
}

setInterval(tryProcess, 1000);

/* ---------------- SOCKET ---------------- */
io.on('connection', (socket) => {
  const user = socket.handshake.query.user || 'anonymous';
  socket.join(user);

  (async () => {
    const stats = await getStats(user);
    socket.emit('stats', stats);
  })();
});

/* ---------------- START ---------------- */
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});


// server.js


// const express = require('express');
// const http = require('http');
// const cors = require('cors');
// const bodyParser = require('body-parser');
// const { Server } = require('socket.io');
// const Redis = require('ioredis');
// const { v4: uuidv4 } = require('uuid');

// /* ---------------- CONFIG ---------------- */
// const PORT = process.env.PORT || 3000;
// const INSTANCE_ID = `session-${PORT}`;

// const CONFIG = {
//   RATE_PER_MIN: Number(process.env.RATE_PER_MIN || 10),
//   MAX_ACTIVE: Number(process.env.MAX_ACTIVE || 5),
// };

// /* ---------------- REDIS ---------------- */
// const redis = new Redis({ host: '127.0.0.1', port: 6379 });

// /* ---------------- HELPERS ---------------- */
// function minuteKey() {
//   const d = new Date();
//   return (
//     d.getUTCFullYear() +
//     String(d.getUTCMonth() + 1).padStart(2, '0') +
//     String(d.getUTCDate()).padStart(2, '0') +
//     String(d.getUTCHours()).padStart(2, '0') +
//     String(d.getUTCMinutes()).padStart(2, '0')
//   );
// }

// function getUserKeys(user) {
//   const NS = `queue:${INSTANCE_ID}:${user}`;
//   return {
//     PENDING_QUEUE: `${NS}:pending`,
//     PROCESSING_LIST: `${NS}:processing`,
//     COMPLETED_LIST: `${NS}:completed`,
//     ACTIVE_CALLS: `${NS}:active`,
//   };
// }

// /* ---------------- LUA SCRIPT ---------------- */
// const luaPop = `
// local active = tonumber(redis.call('GET', KEYS[1]) or '0')
// if active >= tonumber(ARGV[1]) then return nil end

// local rate = tonumber(redis.call('GET', KEYS[2]) or '0')
// if rate >= tonumber(ARGV[2]) then return nil end

// local job = redis.call('LPOP', KEYS[3])
// if not job then return nil end

// redis.call('INCR', KEYS[1])
// redis.call('INCR', KEYS[2])
// redis.call('EXPIRE', KEYS[2], 60)
// redis.call('RPUSH', KEYS[4], job)

// return job
// `;

// /* ---------------- APP ---------------- */
// const app = express();
// const server = http.createServer(app);
// const io = new Server(server, { cors: { origin: '*' } });

// app.use(cors());
// app.use(bodyParser.json({ limit: '30mb' }));
// app.use(express.static(__dirname));

// /* ---------------- UPLOAD ---------------- */
// app.post('/upload', async (req, res) => {
//   const user = req.query.user || 'anonymous';
//   const { records } = req.body;

//   if (!Array.isArray(records)) {
//     return res.status(400).json({ error: 'records must be array' });
//   }

//   const { PENDING_QUEUE } = getUserKeys(user);
//   const pipe = redis.pipeline();

//   for (const r of records) {
//     const id = uuidv4();
//     pipe.hmset(`job:${id}`, {
//       requestId: id,
//       payload: JSON.stringify(r),
//       user,
//       createdAt: Date.now(),
//     });
//     pipe.rpush(PENDING_QUEUE, id);
//   }

//   await pipe.exec();
//   res.json({ uploaded: records.length, user, port: PORT });
// });

// /* ---------------- STATS ---------------- */
// async function getStats(user) {
//   const minute = minuteKey();
//   const { PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, ACTIVE_CALLS } =
//     getUserKeys(user);

//   return {
//     pending: await redis.llen(PENDING_QUEUE),
//     processing: await redis.llen(PROCESSING_LIST),
//     completed: await redis.llen(COMPLETED_LIST),
//     active: Number(await redis.get(ACTIVE_CALLS)) || 0,
//     rateLimit: CONFIG.RATE_PER_MIN,
//     usedThisMin: Number(await redis.get(`global:rate:${minute}`)) || 0,
//     maxActive: CONFIG.MAX_ACTIVE,
//   };
// }

// // app.get('/stats', async (req, res) => {
// //   const user = req.query.user || 'anonymous';
// //   res.json(await getStats(user));
// // });
// app.get("/stats", async (req, res) => {
//   const user = req.query.user || "anonymous";

//   const minute = minuteKey(); // Use same minuteKey() function
//   const RATE_KEY = `queue:${INSTANCE_ID}:${user}:rate:${minute}`;

//   const stats = {
//     pending: await redis.llen(`${user}:pending`),
//     processing: await redis.llen(`${user}:processing`),
//     completed: await redis.llen(`${user}:completed`),
//     active: Number(await redis.get(`${user}:active_calls`)) || 0,
//     rateLimit: CONFIG.RATE_PER_MIN,
//     usedThisMin: Number(await redis.get(RATE_KEY)) || 0,
//     maxActive: CONFIG.MAX_ACTIVE
//   };

//   res.json(stats);
// });

// /* ---------------- EXPORT ---------------- */
// app.get('/export-completed', async (req, res) => {
//   const user = req.query.user || 'anonymous';
//   const { COMPLETED_LIST } = getUserKeys(user);

//   const rows = await redis.lrange(COMPLETED_LIST, 0, -1);
//   res.json(rows.map(r => JSON.parse(r)));
// });

// /* ---------------- RESET ---------------- */
// app.post('/reset', async (req, res) => {
//   const user = req.query.user || 'anonymous';
//   const NS = `queue:${INSTANCE_ID}:${user}`;
//   const keys = await redis.keys(`${NS}:*`);
//   if (keys.length) await redis.del(keys);

//   const stats = await getStats(user);
//   io.to(user).emit('stats', stats);

//   res.json({ message: `Reset done for ${user}` });
// });

// /* ---------------- PROCESSOR ---------------- */
// // async function tryProcess() {
// //   const minute = minuteKey();
// //   // const RATE_KEY = `global:rate:${minute}`;
  

// //   const queues = await redis.keys(`queue:${INSTANCE_ID}:*:pending`);

// //   for (const pendingQueue of queues) {
// //     const user = pendingQueue.split(':')[2];
// //     const { PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, ACTIVE_CALLS } =
// //       getUserKeys(user);

// //       const RATE_KEY = `queue:${INSTANCE_ID}:${user}:rate:${minute}`;

// //     const jobId = await redis.eval(
// //       luaPop,
// //       4,
// //       ACTIVE_CALLS,
// //       RATE_KEY,
// //       PENDING_QUEUE,
// //       PROCESSING_LIST,
// //       CONFIG.MAX_ACTIVE,
// //       CONFIG.RATE_PER_MIN
// //     );

// //     if (!jobId) continue;

// //     // simulate processing delay
// //     await new Promise((r) => setTimeout(r, 1500));

// //     const job = await redis.hgetall(`job:${jobId}`);

// //     await redis
// //       .multi()
// //       .lrem(PROCESSING_LIST, 1, jobId)
// //       .decr(ACTIVE_CALLS)
// //       .rpush(COMPLETED_LIST, JSON.stringify(job))
// //       .exec();

// //     const stats = await getStats(user);
// //     io.to(user).emit('stats', stats);
// //   }
// // }
// async function tryProcess() {
//   const minute = minuteKey();
//   const queues = await redis.keys(`queue:${INSTANCE_ID}:*:pending`);

//   for (const pendingQueue of queues) {
//     const user = pendingQueue.split(':')[2];
//     const { PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, ACTIVE_CALLS } = getUserKeys(user);

//     const RATE_KEY = `queue:${INSTANCE_ID}:${user}:rate:${minute}`; // moved here

//     const jobId = await redis.eval(
//       luaPop,
//       4,
//       ACTIVE_CALLS,
//       RATE_KEY,
//       PENDING_QUEUE,
//       PROCESSING_LIST,
//       CONFIG.MAX_ACTIVE,
//       CONFIG.RATE_PER_MIN
//     );

//     if (!jobId) continue;

//     await new Promise(r => setTimeout(r, 1500));
//     const job = await redis.hgetall(`job:${jobId}`);

//     await redis.multi()
//       .lrem(PROCESSING_LIST, 1, jobId)
//       .decr(ACTIVE_CALLS)
//       .rpush(COMPLETED_LIST, JSON.stringify(job))
//       .exec();

//     const stats = await getStats(user);
//     io.to(user).emit('stats', stats);
//   }
// }

// setInterval(tryProcess, 1000);

// /* ---------------- SOCKET ---------------- */
// io.on('connection', (socket) => {
//   const user = socket.handshake.query.user || 'anonymous';
//   socket.join(user);

//   (async () => {
//     const stats = await getStats(user);
//     socket.emit('stats', stats);
//   })();
// });

// /* ---------------- START ---------------- */
// server.listen(PORT, () => {
//   console.log(`🚀 Server running on port ${PORT}`);
// });
