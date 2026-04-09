# AudioAmp — Multi-Device Audio Sync

Turn nearby devices into a synchronized speaker array. Play the same audio on
multiple phones/laptops/tablets at the same time over WiFi — zero latency.

## How It Works

1. **Start the server** on one machine
2. **Open the URL** on every device connected to the same WiFi
3. **Upload an audio file** from any device
4. **Hit play** — all devices play in perfect sync

### Sync Protocol

- NTP-style clock offset calculation (5-sample median, lowest-RTT selection)
- Server computes per-client offset, then broadcasts `"play at server_time T"`
- Each client converts T to local time and schedules playback via Web Audio API
- 300ms future scheduling window absorbs network jitter

## Quick Start

```bash
npm install
npm start
```

Then open `http://<your-local-ip>:3000` on all devices.

## Tech Stack

| Layer     | Tech                        |
|-----------|-----------------------------|
| Server    | Node.js, Express, ws        |
| Transport | WebSocket                   |
| Audio     | Web Audio API               |
| Sync      | NTP-style clock calibration |
| Upload    | Multer                      |

## Supported Formats

MP3, WAV, OGG, M4A, AAC, FLAC, WebM

## Requirements

- Node.js 18+
- All devices on the same local network (WiFi)
