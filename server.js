const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const AUDIO_DIR = path.join(__dirname, 'public', 'audio');
const PUBLIC_DIR = path.join(__dirname, 'public');
const BASE_PORT = parseInt(process.env.PORT, 10) || 3000;

// Ensure audio directory exists
fs.mkdirSync(AUDIO_DIR, { recursive: true });

// File upload config
const storage = multer.diskStorage({
  destination: AUDIO_DIR,
  filename: (req, file, cb) => {
    const safe = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
    cb(null, `${Date.now()}-${safe}`);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.webm'];
    const ext = path.extname(file.originalname).toLowerCase();
    cb(null, allowed.includes(ext));
  }
});

// ── State ──────────────────────────────────────────────────────────────
let clients = new Map();
let currentTrack = null;
let isPlaying = false;
let hostWs = null;
let nextClientId = 1;

// ── CORS — must be before all routes ──────────────────────────────────
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Request logging (helps debug connectivity) ────────────────────────
app.use((req, res, next) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  console.log(`  HTTP ${req.method} ${req.url} from ${ip}`);
  next();
});

// ── Static files (use __dirname for absolute path) ────────────────────
app.use(express.static(PUBLIC_DIR));
app.use('/audio', express.static(AUDIO_DIR));

// ── API routes ────────────────────────────────────────────────────────
app.post('/upload', upload.single('audio'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No valid audio file' });
  const url = `/audio/${req.file.filename}`;
  currentTrack = { filename: req.file.originalname, url };
  broadcast({ type: 'track-loaded', filename: currentTrack.filename, url });
  res.json({ success: true, filename: req.file.originalname, url });
});

app.get('/tracks', (req, res) => {
  const files = fs.readdirSync(AUDIO_DIR).filter(f => {
    const ext = path.extname(f).toLowerCase();
    return ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.webm'].includes(ext);
  });
  res.json(files.map(f => ({ filename: f, url: `/audio/${f}` })));
});

app.get('/status', (req, res) => {
  res.json({ clients: clients.size, currentTrack, isPlaying });
});

// Health check — clients use this to verify HTTP connectivity before WS
app.get('/health', (req, res) => {
  res.json({ ok: true, time: Date.now(), clients: clients.size, port: actualPort });
});

// Connection info for the UI
app.get('/connection-info', (req, res) => {
  res.json({ ips: getAllLocalIPs(), port: actualPort });
});

// ── WebSocket handling ─────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const clientId = nextClientId++;
  const clientInfo = {
    id: clientId,
    name: `Device ${clientId}`,
    clockOffset: 0,
    ready: false,
    syncSamples: [],
    ip
  };
  clients.set(ws, clientInfo);

  if (!hostWs || !clients.has(hostWs)) {
    hostWs = ws;
  }

  console.log(`[+] Device ${clientId} connected from ${ip} (${clients.size} total)`);

  // Handle WS errors — without this, errors crash the process
  ws.on('error', (err) => {
    console.error(`[!] Device ${clientId} WS error:`, err.message);
  });

  // Send welcome
  send(ws, {
    type: 'welcome',
    clientId,
    isHost: ws === hostWs,
    currentTrack,
    isPlaying,
    totalClients: clients.size
  });

  broadcast({ type: 'client-count', count: clients.size });
  startClockSync(ws);

  // Keepalive — ping every 25s to prevent routers/NATs from killing the connection
  const keepalive = setInterval(() => {
    if (ws.readyState === ws.OPEN) {
      ws.ping();
    } else {
      clearInterval(keepalive);
    }
  }, 25000);

  ws.on('pong', () => {
    // Client is alive — no action needed
  });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', (code, reason) => {
    clearInterval(keepalive);
    const info = clients.get(ws);
    clients.delete(ws);
    console.log(`[-] Device ${info?.id} disconnected (code=${code}, ${clients.size} remaining)`);

    if (ws === hostWs) {
      hostWs = clients.size > 0 ? clients.keys().next().value : null;
      if (hostWs) send(hostWs, { type: 'you-are-host' });
    }
    broadcast({ type: 'client-count', count: clients.size });
    broadcastDeviceList();
  });
});

// Handle WS server-level errors
wss.on('error', (err) => {
  console.error('[!] WebSocket server error:', err.message);
});

function handleMessage(ws, msg) {
  const info = clients.get(ws);
  if (!info) return;

  switch (msg.type) {
    case 'pong': {
      const now = Date.now();
      const rtt = now - msg.t0;
      const offset = msg.clientTime - (msg.t0 + rtt / 2);
      info.syncSamples.push({ offset, rtt });

      if (info.syncSamples.length >= 5) {
        info.syncSamples.sort((a, b) => a.rtt - b.rtt);
        info.clockOffset = info.syncSamples[0].offset;
        info.syncSamples = [];
        send(ws, { type: 'sync-done', offset: info.clockOffset });
        console.log(`  Device ${info.id} clock offset: ${info.clockOffset.toFixed(1)}ms (RTT: ${info.syncSamples.length > 0 ? info.syncSamples[0].rtt : '?'}ms)`);
      } else {
        setTimeout(() => sendPing(ws), 50);
      }
      break;
    }

    case 'play': {
      isPlaying = true;
      const playAtServerTime = Date.now() + 300;
      const seekTo = msg.seekTo || 0;

      for (const [clientWs, clientInfo] of clients) {
        const playAtClientTime = playAtServerTime + clientInfo.clockOffset;
        send(clientWs, { type: 'play-at', time: playAtClientTime, seekTo });
      }
      break;
    }

    case 'pause': {
      isPlaying = false;
      broadcast({ type: 'pause' });
      break;
    }

    case 'stop': {
      isPlaying = false;
      broadcast({ type: 'stop' });
      break;
    }

    case 'select-track': {
      currentTrack = { filename: msg.filename, url: msg.url };
      isPlaying = false;
      broadcast({ type: 'track-loaded', filename: msg.filename, url: msg.url });
      break;
    }

    case 'set-volume': {
      broadcast({ type: 'set-volume', volume: msg.volume });
      break;
    }

    case 'resync': {
      for (const [clientWs] of clients) {
        const cInfo = clients.get(clientWs);
        cInfo.syncSamples = [];
        startClockSync(clientWs);
      }
      break;
    }

    case 'set-name': {
      info.name = msg.name || info.name;
      broadcastDeviceList();
      break;
    }

    case 'ready': {
      info.ready = true;
      broadcastDeviceList();
      break;
    }
  }
}

// ── Clock sync ─────────────────────────────────────────────────────────
function startClockSync(ws) {
  sendPing(ws);
}

function sendPing(ws) {
  if (ws.readyState !== ws.OPEN) return;
  send(ws, { type: 'ping', t0: Date.now() });
}

// ── Broadcast helpers ──────────────────────────────────────────────────
function send(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try {
      ws.send(JSON.stringify(obj));
    } catch (err) {
      console.error('[!] Send error:', err.message);
    }
  }
}

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const [ws] of clients) {
    if (ws.readyState === ws.OPEN) {
      try { ws.send(msg); } catch {}
    }
  }
}

function broadcastDeviceList() {
  const devices = [];
  for (const [ws, info] of clients) {
    devices.push({
      id: info.id,
      name: info.name,
      isHost: ws === hostWs,
      ready: info.ready,
      offset: Math.round(info.clockOffset)
    });
  }
  broadcast({ type: 'device-list', devices });
}

// ── Get ALL local IPs ──────────────────────────────────────────────────
function getAllLocalIPs() {
  const ips = [];
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        ips.push({ name, address: net.address });
      }
    }
  }
  return ips;
}

// ── Start server with port fallback ────────────────────────────────────
let actualPort = BASE_PORT;

function tryListen(port) {
  actualPort = port;
  server.listen(port, '0.0.0.0', () => {
    const ips = getAllLocalIPs();
    console.log('');
    console.log('  ╔══════════════════════════════════════════════════════╗');
    console.log('  ║           AUDIO AMP - Multi-Device Sync             ║');
    console.log('  ╠══════════════════════════════════════════════════════╣');
    console.log(`  ║  Local:     http://localhost:${port}                  ║`);
    if (ips.length > 0) {
      for (const ip of ips) {
        const url = `http://${ip.address}:${port}`;
        console.log(`  ║  ${ip.name.padEnd(9)} ${url.padEnd(40)}║`);
      }
    } else {
      console.log('  ║  WARNING: No network interfaces found!              ║');
      console.log('  ║  Other devices will not be able to connect.         ║');
    }
    console.log('  ╠══════════════════════════════════════════════════════╣');
    console.log('  ║  Open the Network URL on all devices on the same    ║');
    console.log('  ║  WiFi. Or scan the QR code shown on the web page.   ║');
    console.log('  ╚══════════════════════════════════════════════════════╝');
    console.log('');

    if (ips.length === 0) {
      console.log('  TROUBLESHOOTING:');
      console.log('  1. Make sure this machine is connected to WiFi');
      console.log('  2. Run: ip addr show  (Linux) / ifconfig (Mac) / ipconfig (Windows)');
      console.log('  3. Find your local IP (usually 192.168.x.x or 10.x.x.x)');
      console.log(`  4. Open http://<that-ip>:${port} on other devices`);
      console.log('');
    }
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.log(`  Port ${port} is in use, trying ${port + 1}...`);
      server.close();
      tryListen(port + 1);
    } else {
      console.error('Server error:', err);
      process.exit(1);
    }
  });
}

tryListen(BASE_PORT);
