const express = require('express');
const http = require('http');
const { Server: SocketIO } = require('socket.io');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const os = require('os');
const QRCode = require('qrcode');
const localtunnel = require('localtunnel');

// ── Paths ─────────────────────────────────────────────────────────────
const PUBLIC_DIR = path.join(__dirname, 'public');
const AUDIO_DIR = path.join(__dirname, 'public', 'audio');
fs.mkdirSync(AUDIO_DIR, { recursive: true });

// ── Express + HTTP server ─────────────────────────────────────────────
const app = express();
const server = http.createServer(app);

// ── Socket.IO ─────────────────────────────────────────────────────────
const io = new SocketIO(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingInterval: 10000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e7,
  // Required for localtunnel/reverse proxy
  allowEIO3: true
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
const clients = new Map();
let currentTrack = null;
let isPlaying = false;
let hostId = null;
let nextClientId = 1;
let actualPort = 3000;
let tunnelUrl = null;     // Set once localtunnel connects
let tunnelQrDataUrl = null;

// ── Middleware ─────────────────────────────────────────────────────────
// Trust proxy — required for localtunnel reverse proxy
app.set('trust proxy', true);

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Bypass localtunnel's interstitial page
app.use((req, res, next) => {
  if (req.headers['bypass-tunnel-reminder']) {
    // Already handled by the header
  }
  next();
});

app.use(express.static(PUBLIC_DIR));
app.use('/audio', express.static(AUDIO_DIR));

// ── Routes ────────────────────────────────────────────────────────────
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
  const lanUrl = ips[0] ? `http://${ips[0].address}:${actualPort}` : null;

  let lanQr = null;
  if (lanUrl) {
    try { lanQr = await QRCode.toDataURL(lanUrl, { width: 256, margin: 2 }); } catch {}
  }

  res.json({
    ips,
    port: actualPort,
    lanUrl,
    lanQrDataUrl: lanQr,
    tunnelUrl,
    tunnelQrDataUrl,
    tunnelStatus: tunnelUrl ? 'connected' : 'connecting'
  });
});

// ── Socket.IO ─────────────────────────────────────────────────────────
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

  if (!hostId || !clients.has(hostId)) hostId = socket.id;

  const transport = socket.conn.transport.name;
  console.log(`[+] Device ${clientId} via ${transport} (${clients.size} total)`);
  socket.conn.on('upgrade', (t) => {
    console.log(`    Device ${clientId} upgraded to ${t.name}`);
  });

  socket.emit('welcome', {
    clientId,
    isHost: socket.id === hostId,
    currentTrack,
    isPlaying,
    totalClients: clients.size
  });

  io.emit('client-count', { count: clients.size });
  startClockSync(socket);

  socket.on('clock-pong', (data) => {
    const now = Date.now();
    const rtt = now - data.t0;
    const offset = data.clientTime - (data.t0 + rtt / 2);
    info.syncSamples.push({ offset, rtt });

    if (info.syncSamples.length >= 8) {
      info.syncSamples.sort((a, b) => a.rtt - b.rtt);
      const best = info.syncSamples.slice(0, 5);
      info.clockOffset = best.reduce((s, x) => s + x.offset, 0) / best.length;
      info.syncSamples = [];
      socket.emit('sync-done', { offset: info.clockOffset, rtt: best[0].rtt });
      console.log(`    Device ${clientId} synced: offset=${info.clockOffset.toFixed(1)}ms, RTT=${best[0].rtt}ms`);
    } else {
      setTimeout(() => sendPing(socket), 30);
    }
  });

  socket.on('play', (data) => {
    isPlaying = true;
    const playAt = Date.now() + 400;
    const seekTo = data.seekTo || 0;
    for (const [sid, ci] of clients) {
      io.to(sid).emit('play-at', { time: playAt + ci.clockOffset, seekTo });
    }
  });

  socket.on('pause', () => { isPlaying = false; io.emit('pause'); });
  socket.on('stop', () => { isPlaying = false; io.emit('stop'); });

  socket.on('select-track', (data) => {
    currentTrack = { filename: data.filename, url: data.url };
    isPlaying = false;
    io.emit('track-loaded', data);
  });

  socket.on('set-volume', (data) => io.emit('set-volume', data));

  socket.on('resync', () => {
    for (const [sid] of clients) {
      clients.get(sid).syncSamples = [];
      const s = io.sockets.sockets.get(sid);
      if (s) startClockSync(s);
    }
  });

  socket.on('set-name', (data) => { info.name = data.name || info.name; broadcastDeviceList(); });
  socket.on('ready', () => { info.ready = true; broadcastDeviceList(); });

  socket.on('disconnect', (reason) => {
    clients.delete(socket.id);
    console.log(`[-] Device ${clientId} disconnected: ${reason} (${clients.size} left)`);
    if (socket.id === hostId) {
      hostId = clients.keys().next().value || null;
      if (hostId) io.to(hostId).emit('you-are-host');
    }
    io.emit('client-count', { count: clients.size });
    broadcastDeviceList();
  });
});

function startClockSync(socket) { sendPing(socket); }
function sendPing(socket) { if (socket.connected) socket.emit('clock-ping', { t0: Date.now() }); }

function broadcastDeviceList() {
  const devices = [];
  for (const [sid, info] of clients) {
    devices.push({ id: info.id, name: info.name, isHost: sid === hostId, ready: info.ready, offset: Math.round(info.clockOffset) });
  }
  io.emit('device-list', { devices });
}

// ── IP detection ──────────────────────────────────────────────────────
function getNetworkIPs() {
  const ips = [];
  const nets = os.networkInterfaces();
  const sorted = Object.keys(nets).sort((a, b) => {
    const score = (n) => {
      n = n.toLowerCase();
      if (n.includes('wlan') || n.includes('wi-fi') || n.includes('wifi')) return 0;
      if (n.startsWith('en')) return 1;
      if (n.includes('eth')) return 2;
      if (n.includes('docker') || n.includes('veth') || n.includes('br-')) return 10;
      if (n.includes('tun') || n.includes('tap')) return 11;
      return 5;
    };
    return score(a) - score(b);
  });
  for (const name of sorted) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        const n = name.toLowerCase();
        if (n.includes('docker') || n.includes('veth') || n.includes('br-')) continue;
        if (n.includes('tun') || n.includes('tap')) continue;
        if (net.address.startsWith('172.17.')) continue;
        ips.push({ name, address: net.address });
      }
    }
  }
  return ips;
}

// ── Tunnel ────────────────────────────────────────────────────────────
async function startTunnel(port) {
  console.log('  Tunnel: opening...');
  try {
    const tunnel = await localtunnel({ port, allow_invalid_cert: true });
    tunnelUrl = tunnel.url;

    try {
      tunnelQrDataUrl = await QRCode.toDataURL(tunnelUrl, { width: 256, margin: 2 });
    } catch {}

    console.log('');
    console.log(`  ========================================`);
    console.log(`  SHARE THIS URL (works from ANY device):`);
    console.log(`  ${tunnelUrl}`);
    console.log(`  ========================================`);

    try {
      const qr = await QRCode.toString(tunnelUrl, { type: 'terminal', small: true });
      console.log('');
      console.log('  Scan to connect from any device:');
      console.log(qr.split('\n').map(l => '  ' + l).join('\n'));
    } catch {}

    // Notify already-connected clients about the tunnel URL
    io.emit('tunnel-ready', { tunnelUrl, tunnelQrDataUrl });

    tunnel.on('close', () => {
      console.log('  Tunnel: closed, reopening...');
      tunnelUrl = null;
      tunnelQrDataUrl = null;
      setTimeout(() => startTunnel(port), 3000);
    });

    tunnel.on('error', (err) => {
      console.log(`  Tunnel error: ${err.message}`);
    });
  } catch (err) {
    console.log(`  Tunnel failed: ${err.message}`);
    console.log('  Retrying in 5s...');
    setTimeout(() => startTunnel(port), 5000);
  }
}

// ── Start ─────────────────────────────────────────────────────────────
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
      console.log(`  │  LAN:     http://${primary.address}:${port}`.padEnd(53) + '│');
    }
    console.log('  ├──────────────────────────────────────────────────┤');
    console.log('  │  Transport: Socket.IO (WebSocket + polling)      │');
    console.log('  └──────────────────────────────────────────────────┘');

    // Start the tunnel — this is the reliable connection method
    startTunnel(port);
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
