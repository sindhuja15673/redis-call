
const express = require('express');
const http = require('http');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const INSTANCE_ID = `session-${PORT}`;

/* ---------------- DYNAMIC CONFIG ---------------- */
const CONFIG = {
  RATE_PER_MIN: Number(process.env.RATE_PER_MIN || 10),
  MAX_ACTIVE: Number(process.env.MAX_ACTIVE || 5),
};

/* ---------------- REDIS ---------------- */
const redis = new Redis({ host: '127.0.0.1', port: 6379 });

/* ---------------- REDIS NAMESPACE (PER PORT) ---------------- */
const NS = `queue:${INSTANCE_ID}`;

const PENDING_QUEUE = `${NS}:pending`;
const PROCESSING_LIST = `${NS}:processing`;
const COMPLETED_LIST = `${NS}:completed`;
const ACTIVE_CALLS = `${NS}:active`;

/* ---------------- APP ---------------- */
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(bodyParser.json({ limit: '30mb' }));
app.use(express.static(__dirname));

/* ---------------- TIME KEY ---------------- */
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


async function getStats() {
  const minute = minuteKey();
  const RATE_KEY = `${NS}:rate:${minute}`;

  return {
    port: PORT,
    pending: await redis.llen(PENDING_QUEUE),
    processing: await redis.llen(PROCESSING_LIST),
    completed: await redis.llen(COMPLETED_LIST),
    active: Number(await redis.get(ACTIVE_CALLS)) || 0,
    rateLimit: CONFIG.RATE_PER_MIN,
    usedThisMin: Number(await redis.get(RATE_KEY)) || 0,
    maxActive: CONFIG.MAX_ACTIVE
  };
}


/* ---------------- LUA (PER PORT LIMITS) ---------------- */
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

/* ---------------- UPLOAD ---------------- */
app.post('/upload', async (req, res) => {
  const { records } = req.body;
  if (!Array.isArray(records)) {
    return res.status(400).json({ error: 'records must be array' });
  }

  const pipe = redis.pipeline();

  for (const r of records) {
    const id = uuidv4();
    pipe.hmset(`job:${id}`, {
      requestId: id,
      payload: JSON.stringify(r),
      session: INSTANCE_ID,
      createdAt: Date.now()
    });
    pipe.rpush(PENDING_QUEUE, id);
  }

  await pipe.exec();
  res.json({ uploaded: records.length, port: PORT });
});

/* ---------------- STATS ---------------- */
app.get('/stats', async (_, res) => {
  res.json(await getStats());
});


/* ---------------- RESET (SAFE) ---------------- */
app.post('/reset', async (_, res) => {
  const keys = await redis.keys(`${NS}:*`);
  if (keys.length) await redis.del(keys);

  
  const freshStats = await getStats();
  io.emit('stats', freshStats);  

  res.json({ message: `Reset done for ${INSTANCE_ID}` });
});

/* ---------------- EXPORT ---------------- */
app.get('/export-completed', async (_, res) => {
  const rows = await redis.lrange(COMPLETED_LIST, 0, -1);
  res.json(rows.map(r => JSON.parse(r)));
});

/* ---------------- PROCESSOR ---------------- */
async function tryProcess() {
  const minute = minuteKey();
  const RATE_KEY = `${NS}:rate:${minute}`;

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

  if (!jobId) return;

  await new Promise(r => setTimeout(r, 1500));

  const job = await redis.hgetall(`job:${jobId}`);

  await redis.multi()
    .lrem(PROCESSING_LIST, 1, jobId)
    .decr(ACTIVE_CALLS)
    .rpush(COMPLETED_LIST, JSON.stringify(job))
    .exec();

  io.emit('stats', await fetchStats());
}

// async function fetchStats() {
//   return (await fetch(`http://localhost:${PORT}/stats`)).json();
// }
async function fetchStats() {
  return await getStats();
}

setInterval(tryProcess, 1000);

/* ---------------- SOCKET ---------------- */
io.on('connection', socket => {
  fetchStats().then(s => socket.emit('stats', s));
});

/* ---------------- START ---------------- */
server.listen(PORT, () => {
  console.log(
    `🚀 Port ${PORT} | Rate ${CONFIG.RATE_PER_MIN}/min | MaxActive ${CONFIG.MAX_ACTIVE}`
  );
});
