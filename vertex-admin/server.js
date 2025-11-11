// server.js — tiny visual self-test for Cloud Run
// Shows results directly in the browser AND logs to Cloud Run stdout.

// 1) Setup ---------------------------------------------------------------
const express = require('express');              // (1) import express 
const app = express();                           // (2) create app
app.set('trust proxy', true);                    // (3) trust Cloud Run proxy
app.use(express.json());                         // (4) json parser
console.log('=== [SERVER] starting test server ==='); // (5) server boot log

// 2) API: simple test endpoint ------------------------------------------
app.get('/api/test', (req, res) => {
  console.log('=== [SERVER] /api/test hit ===');           // (6) server sees API hit
  const t0 = Date.now();                                   // (7) start timing
  // pretend to do a couple of checks
  const checks = [
    { name: 'Process alive', ok: true },
    { name: 'Env PORT present', ok: !!process.env.PORT },
    { name: 'K_REVISION present', ok: !!process.env.K_REVISION },
  ];
  const allOk = checks.every(c => c.ok);                   // (8) aggregate result
  const payload = {
    ok: allOk,
    checks,
    serverTime: new Date().toISOString(),
    revision: process.env.K_REVISION || 'unknown',
    service: process.env.K_SERVICE || 'unknown',
    latencyMs: Date.now() - t0
  };
  console.log('=== [SERVER] /api/test result:', payload);  // (9) log result
  res.json(payload);                                       // (10) send JSON
});

// 3) UI: one page, styled, renders results in the DIV --------------------
app.get('/', (_req, res) => {
  console.log('=== [SERVER] serving / ==='); // (11)
  res.type('html').send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Cloud Run Test</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    :root { --ok:#0a7b34; --bad:#b00020; --fg:#111; --muted:#667; --bg:#f7f7fa; }
    body { margin:0; font:16px/1.45 system-ui, -apple-system, Segoe UI, Roboto, sans-serif; color:var(--fg); background:var(--bg);}
    .wrap { max-width:780px; margin:40px auto; padding:24px; background:#fff; border-radius:16px; box-shadow:0 8px 30px rgba(0,0,0,.08);}
    h1 { margin:0 0 6px 0; font-size:22px; }
    .muted { color:var(--muted); font-size:14px; }
    .grid { display:grid; grid-template-columns: 180px 1fr; gap:10px 16px; margin-top:16px; }
    .label { color:var(--muted); }
    .pill { display:inline-block; padding:2px 10px; border-radius:999px; font-size:12px; color:#fff; }
    .pill.ok { background:var(--ok); }
    .pill.bad { background:var(--bad); }
    .checks { margin-top:16px; border-collapse:collapse; width:100%; }
    .checks th, .checks td { padding:10px 12px; border-bottom:1px solid #eee; text-align:left; }
    .footer { margin-top:18px; font-size:13px; color:var(--muted); }
    .btn { margin-top:16px; padding:8px 14px; border-radius:8px; border:1px solid #ddd; background:#fafafa; cursor:pointer; }
    pre { margin:0; white-space:pre-wrap; word-break:break-word; font-family:ui-monospace, SFMono-Regular, Menlo, monospace; background:#fbfbff; padding:12px; border-radius:10px; border:1px solid #eee;}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Cloud Run Self-Test</h1>
    <div class="muted">This page calls <code>/api/test</code> and renders the result below.</div>

    <div class="grid">
      <div class="label">Overall</div>
      <div id="overall"><span class="pill bad">pending…</span></div>

      <div class="label">Service</div>
      <div id="service">—</div>

      <div class="label">Revision</div>
      <div id="revision">—</div>

      <div class="label">Server time</div>
      <div id="time">—</div>

      <div class="label">Latency</div>
      <div id="latency">—</div>
    </div>

    <table class="checks" id="checksTbl" aria-label="Checks table">
      <thead><tr><th>Check</th><th>Status</th></tr></thead>
      <tbody></tbody>
    </table>

    <button class="btn" id="rerunBtn">Re-run test</button>

    <div class="footer">Open DevTools → Console to see browser logs. Server logs are in Cloud Run <em>stdout</em>.</div>

    <details style="margin-top:16px;">
      <summary>Raw JSON</summary>
      <pre id="raw">(waiting)</pre>
    </details>
  </div>

  <script>
    console.log('[BROWSER] page script loaded');                // (A) browser boot log
    const els = {
      overall:  document.getElementById('overall'),
      service:  document.getElementById('service'),
      revision: document.getElementById('revision'),
      time:     document.getElementById('time'),
      latency:  document.getElementById('latency'),
      tbody:    document.querySelector('#checksTbl tbody'),
      raw:      document.getElementById('raw'),
      rerun:    document.getElementById('rerunBtn'),
    };

    async function runTest() {
      console.log('[BROWSER] calling /api/test');               // (B)
      els.overall.innerHTML = '<span class="pill bad">running…</span>';
      try {
        const t0 = performance.now();
        const resp = await fetch('/api/test');
        const data = await resp.json();
        const t = Math.max(0, performance.now() - t0).toFixed(1);
        console.log('[BROWSER] got response', data);            // (C)

        // Overall
        els.overall.innerHTML = data.ok
          ? '<span class="pill ok">OK</span>'
          : '<span class="pill bad">FAILED</span>';

        // Header fields
        els.service.textContent  = data.service;
        els.revision.textContent = data.revision;
        els.time.textContent     = new Date(data.serverTime).toLocaleString();
        els.latency.textContent  = data.latencyMs + ' ms (server), ' + t + ' ms (browser)';

        // Checks table
        els.tbody.innerHTML = '';
        (data.checks || []).forEach(c => {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td>'+c.name+'</td><td>' +
            (c.ok ? '<span class="pill ok">OK</span>' : '<span class="pill bad">FAIL</span>') +
          '</td>';
          els.tbody.appendChild(tr);
        });

        // Raw JSON
        els.raw.textContent = JSON.stringify(data, null, 2);

      } catch (e) {
        console.error('[BROWSER] error', e);                    // (D)
        els.overall.innerHTML = '<span class="pill bad">ERROR</span>';
        els.raw.textContent = String(e);
      }
    }

    window.addEventListener('DOMContentLoaded', runTest);       // (E)
    els.rerun.addEventListener('click', runTest);               // (F)
  </script>
</body>
</html>`);
});

// 4) Start ---------------------------------------------------------------
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(\`=== [SERVER] listening on :\${PORT} ===\`); // (12) server listening
});
