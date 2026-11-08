import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { test } from 'node:test';

test('после ограничения запросов и разрыва потока продолжает выдавать JSON', async () => {
  const port = 4200 + Math.floor(Math.random() * 1000);
  const mock = spawn(process.execPath, ['mock-source.mjs'], { env: { ...process.env, MOCK_PORT: String(port) } });
  const errors = [];
  mock.stderr.on('data', (data) => errors.push(String(data)));
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Имитатор не запустился')), 3000);
    mock.stderr.on('data', (data) => {
      if (String(data).includes('Источник:')) { clearTimeout(timer); resolve(); }
    });
  });
  const collector = spawn(process.execPath, ['collector.mjs'], {
    env: { ...process.env, SOURCE_WS_URL: `ws://127.0.0.1:${port}/socket.io/?EIO=4&transport=websocket`, DETAIL_BASE_URL: `http://127.0.0.1:${port}/details/`, REQUEST_GAP_MS: '10' }
  });
  let output = '';
  let diagnostics = '';
  collector.stdout.on('data', (data) => { output += String(data); });
  collector.stderr.on('data', (data) => { diagnostics += String(data); });
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Недостаточно событий: ${output}\n${diagnostics}\n${errors.join('')}`)), 9000);
      const check = () => {
        if (output.trim().split('\n').length >= 4) { clearTimeout(timer); resolve(); }
      };
      collector.stdout.on('data', check);
    });
    const records = output.trim().split('\n').map(JSON.parse);
    assert.equal(records[0].detail.status, 'ok');
    assert.ok(records.every((record) => record.event.id && record.detail.status === 'ok'));
    assert.match(diagnostics, /Лимит источника/);
    assert.match(diagnostics, /переподключение/);
  } finally {
    collector.kill();
    mock.kill();
    await Promise.allSettled([once(collector, 'exit'), once(mock, 'exit')]);
  }
});

