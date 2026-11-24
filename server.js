// Node wrapper for ZevCloud (Automatic/Nixpacks build): answers the platform
// readiness check (GET / -> 200), proxies WebSocket upgrades at /api to the
// local xray process, and keeps the connection alive with a ~25s WS ping.
// xray is downloaded at runtime into the home directory (writable) on first
// start and reused afterwards.
const http = require('http');
const https = require('https');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const AdmZip = require('adm-zip');
const WebSocket = require('ws');

const XRAY_DIR = os.homedir() || os.tmpdir();
const XRAY_BIN = path.join(XRAY_DIR, 'xray');
const XRAY_ZIP = 'Xray-linux-64.zip';
const XRAY_URL = 'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip';
const XRAY_PORT = 10000;
const WS_PATH = '/api';
const LISTEN_PORT = Number(process.env.PORT) || 3000;

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const get = (u) => {
      https.get(u, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          get(res.headers.location);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error('download failed: HTTP ' + res.statusCode));
          return;
        }
        res.pipe(file);
        file.on('finish', () => file.close(resolve));
      }).on('error', reject);
    };
    get(url);
  });
}

async function ensureXray() {
  if (fs.existsSync(XRAY_BIN)) {
    try { fs.chmodSync(XRAY_BIN, 0o755); } catch (e) { /* ignore */ }
    return;
  }
  console.log('[wrapper] xray binary missing, downloading...');
  const zipPath = path.join(XRAY_DIR, XRAY_ZIP);
  await downloadFile(XRAY_URL, zipPath);
  // Extract with pure-JS adm-zip: some containers have no `unzip` binary.
  new AdmZip(zipPath).extractEntryTo('xray', XRAY_DIR, false, true);
  fs.chmodSync(XRAY_BIN, 0o755);
  fs.unlinkSync(zipPath);
  console.log('[wrapper] xray binary ready');
}

let xrayProc = null;
function startXray() {
  xrayProc = spawn(XRAY_BIN, ['run', '-config', 'config.json'], { cwd: __dirname, stdio: 'inherit' });
  xrayProc.on('exit', (code) => {
    console.log(`[wrapper] xray exited (${code}), restarting in 5s...`);
    setTimeout(startXray, 5000);
  });
}

const server = http.createServer((req, res) => {
  // Readiness check: platform expects a 2xx on the health path (default /).
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok');
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = (req.url || '').split('?')[0];
  if (url !== WS_PATH) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (clientWs) => {
    const upstream = new WebSocket(`ws://127.0.0.1:${XRAY_PORT}${WS_PATH}`, {
      headers: { Host: req.headers.host || '' },
      perMessageDeflate: false,
    });
    let alive = true;
    const pingTimer = setInterval(() => {
      if (!alive) return;
      try { clientWs.ping(); } catch (e) { /* ignore */ }
    }, 25000);
    const cleanup = () => {
      alive = false;
      clearInterval(pingTimer);
      try { clientWs.close(); } catch (e) {}
      try { upstream.close(); } catch (e) {}
    };
    upstream.on('open', () => {
      clientWs.on('message', (data) => { if (upstream.readyState === WebSocket.OPEN) upstream.send(data); });
      upstream.on('message', (data) => { if (clientWs.readyState === WebSocket.OPEN) clientWs.send(data); });
    });
    upstream.on('error', cleanup);
    clientWs.on('close', cleanup);
    clientWs.on('error', cleanup);
    upstream.on('close', cleanup);
  });
});

ensureXray()
  .then(() => startXray())
  .catch((e) => {
    console.error('[wrapper] failed to prepare xray:', e);
    process.exit(1);
  });

server.listen(LISTEN_PORT, '0.0.0.0', () => {
  console.log(`[wrapper] listening on 0.0.0.0:${LISTEN_PORT}, proxying ${WS_PATH} -> 127.0.0.1:${XRAY_PORT}`);
});
