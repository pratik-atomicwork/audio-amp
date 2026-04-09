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

const PORT = process.env.PORT || 3000;
const AUDIO_DIR = path.join(__dirname, 'public', 'audio');

// Ensure audio directory exists
fs.mkdirSync(AUDIO_DIR, { recursive: true });

// File upload config
const storage = multer.diskStorage({
  destination: AUDIO_DIR,
  filename: (req, file, cb) => {
    // Sanitize: keep original name but avoid path traversal
    const safe = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
    cb(null, `${Date.now()}-${safe}`);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB max
  fileFilter: (req, file, cb) => {
    const allowed = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.webm'];
    const ext = path.extname(file.originalname).toLowerCase();
    cb(null, allowed.includes(ext));
  }
});

// ── State ──────────────────────────────────────────────────────────────
let clients = new Map(); // ws → { id, name, clockOffset, ready }
let currentTrack = null; // { filename, url }
let isPlaying = false;
let hostWs = null; // first connected client becomes host
let nextClientId = 1;

// ── Express routes ─────────────────────────────────────────────────────
app.use(express.static('public'));
app.use('/audio', express.static(AUDIO_DIR));

app.post('/upload', upload.single('audio'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No valid audio file' });
  const url = `/audio/${req.file.filename}`;
  currentTrack = { filename: req.file.originalname, url };
  // Notify all clients about new track
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
  res.json({
    clients: clients.size,
    currentTrack,
    isPlaying
  });
});

// ── WebSocket handling ─────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const clientId = nextClientId++;
  const clientInfo = {
    id: clientId,
    name: `Device ${clientId}`,
    clockOffset: 0,
    ready: false,
    syncSamples: []
  };
  clients.set(ws, clientInfo);

  // Assign host if none
  if (!hostWs || !clients.has(hostWs)) {
    hostWs = ws;
  }

  console.log(`[+] Device ${clientId} connected (${clients.size} total)`);

  // Send welcome with current state
  send(ws, {
    type: 'welcome',
    clientId,
    isHost: ws === hostWs,
    currentTrack,
    isPlaying,
    totalClients: clients.size
  });

  // Notify everyone of updated client count
  broadcast({ type: 'client-count', count: clients.size });

  // Start clock sync immediately
  startClockSync(ws);

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    clients.delete(ws);
    console.log(`[-] Device ${info?.id} disconnected (${clients.size} total)`);

    // Reassign host
    if (ws === hostWs) {
      hostWs = clients.size > 0 ? clients.keys().next().value : null;
      if (hostWs) {
        send(hostWs, { type: 'you-are-host' });
      }
    }
    broadcast({ type: 'client-count', count: clients.size });
  });
});

function handleMessage(ws, msg) {
  const info = clients.get(ws);
  if (!info) return;

  switch (msg.type) {
    // ── Clock sync response from client ──
    case 'pong': {
      const now = Date.now();
      const rtt = now - msg.t0;
      const offset = msg.clientTime - (msg.t0 + rtt / 2);
      info.syncSamples.push({ offset, rtt });

      // Collect 5 samples, take median offset
      if (info.syncSamples.length >= 5) {
        info.syncSamples.sort((a, b) => a.rtt - b.rtt);
        // Use the sample with lowest RTT for best accuracy
        info.clockOffset = info.syncSamples[0].offset;
        info.syncSamples = [];
        send(ws, { type: 'sync-done', offset: info.clockOffset });
        console.log(`  Device ${info.id} clock offset: ${info.clockOffset.toFixed(1)}ms`);
      } else {
        // Send next ping
        setTimeout(() => sendPing(ws), 50);
      }
      break;
    }

    // ── Host commands ──
    case 'play': {
      isPlaying = true;
      // Schedule play 200ms in the future to give all clients time
      const playAtServerTime = Date.now() + 300;
      const seekTo = msg.seekTo || 0;

      for (const [clientWs, clientInfo] of clients) {
        // Convert server time to each client's local time
        const playAtClientTime = playAtServerTime + clientInfo.clockOffset;
        send(clientWs, {
          type: 'play-at',
          time: playAtClientTime,
          seekTo
        });
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
      // Re-run clock sync for all clients
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
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const [ws] of clients) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
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

// ── Get local IP ───────────────────────────────────────────────────────
function getLocalIP() {
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        return net.address;
      }
    }
  }
  return 'localhost';
}

// ── Start server ───────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  const ip = getLocalIP();
  console.log('');
  console.log('  ╔══════════════════════════════════════════════╗');
  console.log('  ║         🔊  AUDIO AMP - Multi-Device Sync   ║');
  console.log('  ╠══════════════════════════════════════════════╣');
  console.log(`  ║  Local:   http://localhost:${PORT}             ║`);
  console.log(`  ║  Network: http://${ip}:${PORT}       ║`);
  console.log('  ╠══════════════════════════════════════════════╣');
  console.log('  ║  Open this URL on all devices on the same   ║');
  console.log('  ║  WiFi network to sync audio playback!       ║');
  console.log('  ╚══════════════════════════════════════════════╝');
  console.log('');
});
