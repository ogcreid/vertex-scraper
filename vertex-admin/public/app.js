// public/app.js
// Simple front-end script for the admin panel
// – Stores dbname from input
// – Calls backend API endpoints
// – Renders results

// === tiny helper to get element by CSS selector
function $(sel) {
  return document.querySelector(sel);
}

// === toast helper
function toast(msg) {
  const t = document.createElement('div');
  t.textContent = msg;
  t.style.position = 'fixed';
  t.style.bottom = '20px';
  t.style.right = '20px';
  t.style.background = '#333';
  t.style.color = '#fff';
  t.style.padding = '8px 12px';
  t.style.borderRadius = '4px';
  t.style.zIndex = 9999;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

// === current dbname
let currentDbName = '';

// === generic API wrapper with X-DB-Name header
async function api(url, opts = {}) {
  const headers = opts.headers || {};
  headers['Content-Type'] = 'application/json';
  if (currentDbName) headers['X-DB-Name'] = currentDbName;
  const res = await fetch(url, { ...opts, headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  // for 204 No Content just return {}
  if (res.status === 204) return {};
  return res.json();
}

// === handle dbname form submit
$('#set-dbname').addEventListener('click', () => {
  const db = $('#dbname').value.trim();
  if (!db) {
    toast('Please enter a database name');
    return;
  }
  currentDbName = db;
  toast(`Using database: ${db}`);
});

// === App Config: Load
$('#load-config').addEventListener('click', async () => {
  try {
    const data = await api('/api/app-config');
    $('#config-output').textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    console.error(err);
    toast(err.message);
  }
});

// === App Config: Save
$('#save-config').addEventListener('click', async () => {
  try {
    // Example payload — replace with real form fields later
    const patch = {
      language_exclusions: ["es","fr","de"],
      defaults: { sitemap_mode: "boost", max_depth_from_seed: 3 }
    };
    await api('/api/app-config', {
      method: 'PUT',
      body: JSON.stringify(patch)
    });
    toast('Config saved');
    // refresh view
    const data = await api('/api/app-config');
    $('#config-output').textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    console.error(err);
    toast(err.message);
  }
});

// === Sitemap Sources: Refresh
$('#load-sources').addEventListener('click', async () => {
  try {
    const rows = await api('/api/sources');
    if (!Array.isArray(rows) || rows.length === 0) {
      $('#sources-table').innerHTML = '<em>No sources found.</em>';
      return;
    }
    // build a tiny table
    const header = `
      <tr>
        <th>ID</th>
        <th>Active</th>
        <th>Index URL</th>
        <th>Include Pattern</th>
        <th>Description</th>
      </tr>`;
    const body = rows.map(r => `
      <tr>
        <td>${r.id}</td>
        <td>${r.is_active ? '✅' : '❌'}</td>
        <td>${escapeHtml(r.index_url || '')}</td>
        <td><code>${escapeHtml(r.include_pattern || '')}</code></td>
        <td>${escapeHtml(r.description || '')}</td>
      </tr>`).join('');
    $('#sources-table').innerHTML = `<table class="grid">${header}${body}</table>`;
  } catch (err) {
    console.error(err);
    toast(err.message);
  }
});

// === escape HTML helper
function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}