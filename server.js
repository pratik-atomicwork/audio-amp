const express = require('express');
const http = require('http');
const { Server: SocketIO } = require('socket.io');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const QRCode = require('qrcode');

// ── Paths ─────────────────────────────────────────────────────────────
const PUBLIC_DIR = path.join(__dirname, 'public');
const AUDIO_DIR = path.join(__dirname, 'public', 'audio');
fs.mkdirSync(AUDIO_DIR, { recursive: true });

// ── Express + HTTP server ─────────────────────────────────────────────
const app = express();
const server = http.createServer(app);

// ── Socket.IO — the robust replacement for raw ws ─────────────────────
// Socket.IO handles: WebSocket + HTTP long-polling fallback, automatic
// reconnection, heartbeat keepalive, CORS, proxy traversal, buffering.
const io = new SocketIO(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  // Allow both transports — falls back to polling if WS is blocked
  transports: ['websocket', 'polling'],
  // Ping every 10s, timeout after 20s — keeps connections alive through NATs
  pingInterval: 10000,
  pingTimeout: 20000,
  // Allow large payloads for audio metadata
  maxHttpBufferSize: 1e7
});

// ── File upload ───────────────────────────────────────────────────────
const ALLOWED_EXTS = ['.mp3', '.wav', '.ogg', '.m4a', '.aac', '.flac', '.webm'];
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
    const ext = path.extname(file.originalname).toLowerCase();
    cb(null, ALLOWED_EXTS.includes(ext));
  }
});

// ── State ─────────────────────────────────────────────────────────────
const clients = new Map(); // socket.id → { id, name, clockOffset, ready }
let currentTrack = null;
let isPlaying = false;
let hostId = null;
let nextClientId = 1;
let actualPort = 3000;

// ── Express middleware & routes ────────────────────────────────────────
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.static(PUBLIC_DIR));
app.use('/audio', express.static(AUDIO_DIR));

app.post('/upload', upload.single('audio'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No valid audio file' });
  const url = `/audio/${req.file.filename}`;
  currentTrack = { filename: req.file.originalname, url };
  io.emit('track-loaded', { filename: currentTrack.filename, url });
  res.json({ success: true, filename: req.file.originalname, url });
});

app.get('/tracks', (req, res) => {
  const files = fs.readdirSync(AUDIO_DIR).filter(f =>
    ALLOWED_EXTS.includes(path.extname(f).toLowerCase())
  );
  res.json(files.map(f => ({ filename: f, url: `/audio/${f}` })));
});

app.get('/health', (req, res) => {
  res.json({ ok: true, time: Date.now(), clients: clients.size, port: actualPort });
});

app.get('/connection-info', async (req, res) => {
  const ips = getNetworkIPs();
  const primary = ips[0];
  let qrDataUrl = null;
  if (primary) {
    try {
      qrDataUrl = await QRCode.toDataURL(`http://${primary.address}:${actualPort}`, {
        width: 256, margin: 2, color: { dark: '#000', light: '#fff' }
      });
    } catch {}
  }
  res.json({ ips, port: actualPort, qrDataUrl });
});

// ── Socket.IO connection handling ─────────────────────────────────────
io.on('connection', (socket) => {
  const clientId = nextClientId++;
  const info = {
    id: clientId,
    name: `Device ${clientId}`,
    clockOffset: 0,
    ready: false,
    syncSamples: []
  };
  clients.set(socket.id, info);

  // Assign host if needed
  if (!hostId || !clients.has(hostId)) {
    hostId = socket.id;
  }

  const transport = socket.conn.transport.name; // 'websocket' or 'polling'
  console.log(`[+] Device ${clientId} connected via ${transport} (${clients.size} total)`);

  // Log transport upgrades (polling → websocket)
  socket.conn.on('upgrade', (t) => {
    console.log(`    Device ${clientId} upgraded to ${t.name}`);
  });

  // Send welcome
  socket.emit('welcome', {
    clientId,
    isHost: socket.id === hostId,
    currentTrack,
    isPlaying,
    totalClients: clients.size
  });

  io.emit('client-count', { count: clients.size });
  startClockSync(socket);

  // ── Clock sync ────────────────────────────────────
  socket.on('clock-pong', (data) => {
    const now = Date.now();
    const rtt = now - data.t0;
    const offset = data.clientTime - (data.t0 + rtt / 2);
    info.syncSamples.push({ offset, rtt });

    if (info.syncSamples.length >= 8) {
      // Use median of best 5 RTTs for accuracy
      info.syncSamples.sort((a, b) => a.rtt - b.rtt);
      const best = info.syncSamples.slice(0, 5);
      info.clockOffset = best.reduce((sum, s) => sum + s.offset, 0) / best.length;
      info.syncSamples = [];
      socket.emit('sync-done', { offset: info.clockOffset, rtt: best[0].rtt });
      console.log(`    Device ${clientId} synced: offset=${info.clockOffset.toFixed(1)}ms, bestRTT=${best[0].rtt}ms`);
    } else {
      setTimeout(() => sendPing(socket), 30);
    }
  });

  // ── Playback controls ─────────────────────────────
  socket.on('play', (data) => {
    isPlaying = true;
    const playAtServerTime = Date.now() + 400;
    const seekTo = data.seekTo || 0;

    for (const [sid, cInfo] of clients) {
      const playAtClientTime = playAtServerTime + cInfo.clockOffset;
      io.to(sid).emit('play-at', { time: playAtClientTime, seekTo });
    }
  });

  socket.on('pause', () => {
    isPlaying = false;
    io.emit('pause');
  });

  socket.on('stop', () => {
    isPlaying = false;
    io.emit('stop');
  });

  socket.on('select-track', (data) => {
    currentTrack = { filename: data.filename, url: data.url };
    isPlaying = false;
    io.emit('track-loaded', { filename: data.filename, url: data.url });
  });

  socket.on('set-volume', (data) => {
    io.emit('set-volume', { volume: data.volume });
  });

  socket.on('resync', () => {
    for (const [sid] of clients) {
      const cInfo = clients.get(sid);
      cInfo.syncSamples = [];
      const s = io.sockets.sockets.get(sid);
      if (s) startClockSync(s);
    }
  });

  socket.on('set-name', (data) => {
    info.name = data.name || info.name;
    broadcastDeviceList();
  });

  socket.on('ready', () => {
    info.ready = true;
    broadcastDeviceList();
  });

  // ── Disconnect ────────────────────────────────────
  socket.on('disconnect', (reason) => {
    clients.delete(socket.id);
    console.log(`[-] Device ${clientId} disconnected: ${reason} (${clients.size} remaining)`);

    if (socket.id === hostId) {
      const firstKey = clients.keys().next().value;
      hostId = firstKey || null;
      if (hostId) {
        io.to(hostId).emit('you-are-host');
      }
    }
    io.emit('client-count', { count: clients.size });
    broadcastDeviceList();
  });
});

// ── Clock sync helpers ────────────────────────────────────────────────
function startClockSync(socket) {
  sendPing(socket);
}

function sendPing(socket) {
  if (socket.connected) {
    socket.emit('clock-ping', { t0: Date.now() });
  }
}

// ── Broadcast device list ─────────────────────────────────────────────
function broadcastDeviceList() {
  const devices = [];
  for (const [sid, info] of clients) {
    devices.push({
      id: info.id,
      name: info.name,
      isHost: sid === hostId,
      ready: info.ready,
      offset: Math.round(info.clockOffset)
    });
  }
  io.emit('device-list', { devices });
}

// ── Network IP detection (filters VPN, Docker, etc.) ──────────────────
function getNetworkIPs() {
  const ips = [];
  const nets = os.networkInterfaces();

  // Priority order: wlan/wifi first, then eth, then others
  const sorted = Object.keys(nets).sort((a, b) => {
    const score = (name) => {
      const n = name.toLowerCase();
      if (n.includes('wlan') || n.includes('wi-fi') || n.includes('wifi')) return 0;
      if (n.includes('en0') || n.includes('en1')) return 1; // macOS WiFi
      if (n.includes('eth')) return 2;
      if (n.includes('bridge') || n.includes('docker') || n.includes('veth')) return 10;
      if (n.includes('tun') || n.includes('tap') || n.includes('vpn')) return 11;
      return 5;
    };
    return score(a) - score(b);
  });

  for (const name of sorted) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        // Skip Docker, VPN, and virtual interfaces
        const n = name.toLowerCase();
        if (n.includes('docker') || n.includes('veth') || n.includes('br-')) continue;
        if (n.includes('tun') || n.includes('tap')) continue;
        if (net.address.startsWith('172.17.')) continue; // Docker default
        ips.push({ name, address: net.address });
      }
    }
  }
  return ips;
}

// ── Start server with port fallback ───────────────────────────────────
const BASE_PORT = parseInt(process.env.PORT, 10) || 3000;

function tryListen(port) {
  actualPort = port;
  server.listen(port, '0.0.0.0', async () => {
    const ips = getNetworkIPs();
    const primary = ips[0];

    console.log('');
    console.log('  ┌──────────────────────────────────────────────────┐');
    console.log('  │         AUDIO AMP  -  Multi-Device Sync          │');
    console.log('  ├──────────────────────────────────────────────────┤');
    console.log(`  │  Local:   http://localhost:${port}                 │`);
    if (primary) {
      console.log(`  │  Network: http://${primary.address}:${port}`.padEnd(53) + '│');
    }
    if (ips.length > 1) {
      for (const ip of ips.slice(1)) {
        console.log(`  │  Alt:     http://${ip.address}:${port} (${ip.name})`.padEnd(53) + '│');
      }
    }
    console.log('  ├──────────────────────────────────────────────────┤');
    console.log('  │  Transport: Socket.IO (WebSocket + polling)      │');
    console.log('  │  Open the Network URL on all devices on same     │');
    console.log('  │  WiFi to sync audio playback.                    │');
    console.log('  └──────────────────────────────────────────────────┘');

    if (primary) {
      try {
        const qr = await QRCode.toString(`http://${primary.address}:${port}`, { type: 'terminal', small: true });
        console.log('');
        console.log('  Scan to connect:');
        console.log(qr.split('\n').map(l => '  ' + l).join('\n'));
      } catch {}
    }

    if (ips.length === 0) {
      console.log('');
      console.log('  WARNING: No network interfaces found!');
      console.log('  Make sure this machine is on WiFi, then restart.');
    }

    console.log('');
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.log(`  Port ${port} in use, trying ${port + 1}...`);
      tryListen(port + 1);
    } else {
      console.error('Server error:', err);
      process.exit(1);
    }
  });
}

tryListen(BASE_PORT);
