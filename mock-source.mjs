import http from 'node:http';
import { createHash } from 'node:crypto';

const port = Number(process.env.MOCK_PORT || 4187);
let nextId = 1;
const attempts = new Map();

function frame(data) {
  const body = Buffer.from(data);
  if (body.length >= 126) throw new Error('Демо-кадр слишком длинный');
  return Buffer.concat([Buffer.from([0x81, body.length]), body]);
}

const server = http.createServer((request, response) => {
  if (!request.url.startsWith('/details/')) {
    response.writeHead(404).end();
    return;
  }
  const id = decodeURIComponent(request.url.slice('/details/'.length));
  const count = (attempts.get(id) || 0) + 1;
  attempts.set(id, count);
  if (count === 1) {
    response.writeHead(429, { 'retry-after': '1' }).end('Too many requests');
  } else {
    response.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
    response.end(JSON.stringify({ id, price: 2.14, available: true }));
  }
});

server.on('upgrade', (request, socket) => {
  if (!request.url.startsWith('/socket.io/')) { socket.destroy(); return; }
  const key = request.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = createHash('sha1').update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.write(frame('0{"sid":"demo","upgrades":[],"pingInterval":25000,"pingTimeout":20000}'));
  let sent = 0;
  let buffer = Buffer.alloc(0);
  let timer;
  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 6) {
      const length = buffer[1] & 0x7f;
      if (length > 125 || buffer.length < 6 + length) break;
      const mask = buffer.subarray(2, 6);
      const payload = Buffer.from(buffer.subarray(6, 6 + length));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
      buffer = buffer.subarray(6 + length);
      if (payload.toString() === '40' && !timer) {
        socket.write(frame('40'));
        timer = setInterval(() => {
          if (socket.destroyed) return;
          const event = { id: `match-${nextId++}`, score: `${sent}:0` };
          socket.write(frame(`42["live_update",${JSON.stringify(event)}]`));
          if (++sent === 3) { clearInterval(timer); socket.destroy(); }
        }, 300);
      }
    }
  });
  socket.on('close', () => clearInterval(timer));
});

server.listen(port, '127.0.0.1', () => console.error(`[mock] Источник: http://127.0.0.1:${port}`));
