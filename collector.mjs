import { setTimeout as delay } from 'node:timers/promises';

const wsUrl = process.env.SOURCE_WS_URL || 'ws://127.0.0.1:4187/socket.io/?EIO=4&transport=websocket';
const detailBase = process.env.DETAIL_BASE_URL || 'http://127.0.0.1:4187/details/';
const requestGapMs = Number(process.env.REQUEST_GAP_MS || 500);
const maxQueue = 100;
const log = (message) => process.stderr.write(`[collector] ${message}\n`);
const queue = [];
const queued = new Set();
let draining = false;
let reconnectMs = 250;

process.stdout.on('error', (error) => {
  if (error.code === 'EPIPE') process.exit(0);
  else throw error;
});

function emit(record) {
  process.stdout.write(`${JSON.stringify(record)}\n`);
}

async function fetchDetail(id) {
  let pause = 400;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await fetch(`${detailBase}${encodeURIComponent(id)}`, {
        signal: AbortSignal.timeout(5000),
        headers: { accept: 'application/json' }
      });
      const body = await response.text();
      if (response.status === 429 || /too many requests/i.test(body)) {
        log(`Лимит источника для ${id}; пауза, попытка ${attempt}/3`);
        if (attempt === 3) return { status: 'rate_limited' };
        const retryAfter = Number(response.headers.get('retry-after'));
        await delay(Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : pause);
        pause *= 2;
        continue;
      }
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return { status: 'ok', data: JSON.parse(body) };
    } catch (error) {
      log(`Ошибка деталей ${id}: ${error.message}`);
      if (attempt === 3) return { status: 'unavailable' };
      await delay(pause);
      pause *= 2;
    }
  }
}

async function drain() {
  if (draining) return;
  draining = true;
  while (queue.length) {
    const event = queue.shift();
    const detail = await fetchDetail(event.id);
    emit({ received_at: new Date().toISOString(), event, detail });
    queued.delete(event.id);
    await delay(requestGapMs);
  }
  draining = false;
}

function handle(raw, socket) {
  const value = String(raw);
  if (value.startsWith('0')) { socket.send('40'); return; }
  if (value === '2') { socket.send('3'); return; }
  if (value === '40') return;
  try {
    let event;
    if (value.startsWith('42')) {
      const [name, payload] = JSON.parse(value.slice(2));
      if (name !== 'live_update') return;
      event = payload;
    } else {
      event = JSON.parse(value);
    }
    if (!event || typeof event.id !== 'string') return;
    if (queued.has(event.id) || queue.length >= maxQueue) return;
    queued.add(event.id);
    queue.push(event);
    void drain();
  } catch (error) {
    log(`Некорректное сообщение: ${error.message}`);
  }
}

function connect() {
  log(`Подключение к ${wsUrl}`);
  const socket = new WebSocket(wsUrl);
  socket.addEventListener('open', () => { reconnectMs = 250; log('WebSocket подключён'); });
  socket.addEventListener('message', (message) => handle(message.data, socket));
  socket.addEventListener('error', () => log('Ошибка WebSocket; ожидаю переподключения'));
  socket.addEventListener('close', () => {
    const pause = reconnectMs;
    reconnectMs = Math.min(reconnectMs * 2, 10_000);
    log(`Соединение закрыто; переподключение через ${pause} мс`);
    setTimeout(connect, pause);
  });
}

connect();
