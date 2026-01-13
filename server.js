
// server.js
const express = require('express');
const http = require('http');
const path = require('path');
const cors = require('cors');
const bodyParser = require('body-parser');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const INSTANCE_ID = process.env.INSTANCE_ID || `session-${PORT}`;

const app = express(); // ✅ MUST COME FIRST
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const redis = new Redis({ host: '127.0.0.1', port: 6379 });

/* ---------------- MIDDLEWARE ---------------- */
app.use(cors());
app.use(bodyParser.json({ limit: '30mb' }));
app.use(express.static(path.join(__dirname, 'public')));

/* ---------------- CONFIG ---------------- */
const GLOBAL_RATE_PER_MIN = 10;
const MAX_ACTIVE_CALLS = 100;

const ACTIVE_INSTANCES = 'active_instances';
const PENDING_QUEUE = 'pending_queue';
const PROCESSING_LIST = 'processing_list';
const COMPLETED_LIST = 'completed_list';
const ACTIVE_CALLS = 'active_calls';

/* ---------------- ROOT ---------------- */
app.get('/', (_, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/* ---------------- TIME ---------------- */
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

/* ---------------- LUA ---------------- */
const luaPop = `
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
if active >= tonumber(ARGV[1]) then return nil end

local global = tonumber(redis.call('GET', KEYS[2]) or '0')
if global >= tonumber(ARGV[2]) then return nil end

local job = redis.call('LPOP', KEYS[3])
if not job then return nil end

redis.call('INCR', KEYS[1])
redis.call('INCR', KEYS[2])
redis.call('RPUSH', KEYS[4], job)
redis.call('EXPIRE', KEYS[2], 60)

return job
`;
/* ---------------- EXPORT COMPLETED ---------------- */
app.get('/export-completed', async (req, res) => {
  try {
    const completedIds = await redis.lrange(COMPLETED_LIST, 0, -1);
    const completedJobs = completedIds.map(id => {
      try {
        return JSON.parse(id);
      } catch (e) {
        return id; // fallback in case not JSON
      }
    });
    res.json(completedJobs);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Failed to fetch completed jobs' });
  }
});

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
      id,
      payload: JSON.stringify(r),
      session: INSTANCE_ID,
      createdAt: Date.now()
    });
    pipe.rpush(PENDING_QUEUE, id);
  }

  await pipe.exec();
  res.json({ message: 'Uploaded', count: records.length });
});

/* ---------------- STATS ---------------- */
app.get('/stats', async (_, res) => {
  const minute = minuteKey();

  res.json({
    pending: await redis.llen(PENDING_QUEUE),
    processing: await redis.llen(PROCESSING_LIST),
    completed: await redis.llen(COMPLETED_LIST),
    active: Number(await redis.get(ACTIVE_CALLS)) || 0,
    globalRatePerMin: GLOBAL_RATE_PER_MIN,
    globalRateThisMin: Number(await redis.get(`rate:global:${minute}`)) || 0,
    perSessionLimit: GLOBAL_RATE_PER_MIN,
    sessionRates: { A: 0, B: 0 }
  });
});

/* ---------------- RESET ---------------- */
app.post('/reset', async (_, res) => {
  await redis.flushall();
  await redis.set(ACTIVE_CALLS, 0);
  await redis.sadd(ACTIVE_INSTANCES, INSTANCE_ID);
  res.json({ message: 'System reset' });
});

/* ---------------- PROCESSING ---------------- */
async function tryProcess() {
  const minute = minuteKey();

  const jobId = await redis.eval(
    luaPop,
    4,
    ACTIVE_CALLS,
    `rate:global:${minute}`,
    PENDING_QUEUE,
    PROCESSING_LIST,
    MAX_ACTIVE_CALLS,
    GLOBAL_RATE_PER_MIN
  );

  if (!jobId) return;

  const job = await redis.hgetall(`job:${jobId}`);
  await new Promise(r => setTimeout(r, 1500));

  await redis.multi()
    .lrem(PROCESSING_LIST, 1, jobId)
    .decr(ACTIVE_CALLS)
    .rpush(COMPLETED_LIST, JSON.stringify(job))
    .exec();

  io.emit('stats', await fetchStats());
}

async function fetchStats() {
  return (await fetch(`http://localhost:${PORT}/stats`)).json();
}

setInterval(() => tryProcess(), 1000);

/* ---------------- SOCKET ---------------- */
io.on('connection', socket => {
  socket.emit('stats', {});
});

/* ---------------- START ---------------- */
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT} | ${INSTANCE_ID}`);
});
