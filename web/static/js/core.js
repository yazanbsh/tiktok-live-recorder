const API = '';

// ── Navigation ───────────────────────────────────────────────────────────────
function showPanel(name) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('panel-' + name).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n => {
    if (n.getAttribute('onclick') === `showPanel('${name}')`) n.classList.add('active');
  });
  const titles = {
    watchlist:      'Watchlist',
    recordings:     'TK Recordings',
    'yt-recordings':'YT Recordings',
    downloader:     'Downloader',
    downloads:      'Downloads',
    logs:           'System Log',
  };
  document.getElementById('page-title').textContent = titles[name] || name;

  if (name === 'recordings')      loadRecordings();
  if (name === 'yt-recordings')   loadYTRecordings();
  if (name === 'downloads')       loadDownloadsList();
  if (name === 'logs')            loadLogs();
}

function refreshAll() {
  loadWatchlist();
  loadStats();
  loadYTWatchlist();
  loadYTStats();
}

// ── API helpers ───────────────────────────────────────────────────────────────
async function apiFetch(path, opts = {}) {
  const res = await fetch(API + path, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || res.statusText);
  }
  return res.json();
}

// ── Stats ─────────────────────────────────────────────────────────────────────
async function loadStats() {
  try {
    const s = await apiFetch('/api/stats');
    document.getElementById('stat-total').textContent     = s.total_users;
    document.getElementById('stat-recording').textContent = s.currently_recording;
    document.getElementById('stat-clips').textContent     = s.total_recordings;
    document.getElementById('stat-disk').textContent      = s.disk_used_mb + ' MB';
  } catch(e) { /* silent */ }
}

// ── Logs ──────────────────────────────────────────────────────────────────────
async function loadLogs() {
  const container = document.getElementById('log-lines');
  try {
    const data = await apiFetch('/api/logs?lines=200');
    container.innerHTML = data.lines.map(line => {
      const cls = line.includes('[!]') || line.includes('ERROR') ? 'error'
                : line.includes('WARNING') ? 'warn' : 'info';
      return `<span class="log-line ${cls}">${escHtml(line)}</span>`;
    }).join('\n');
    if (document.getElementById('log-autoscroll').checked) {
      container.scrollTop = container.scrollHeight;
    }
  } catch(e) {
    container.innerHTML = `<span class="log-line error">Failed to load logs: ${e.message}</span>`;
  }
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg, type = 'info') {
  const icons = { success: '✓', error: '✕', info: '◎' };
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${icons[type]}</span><span>${msg}</span>`;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

// ── Video Modal (shared) ──────────────────────────────────────────────────────
let _plyrInstance = null;

function openVideoModal(username, filename, type = 'rec') {
  const baseMap = { rec: '/api/recordings', dl: '/api/tiktok/downloads', yt: '/api/yt/recordings' };
  const base = baseMap[type] || '/api/recordings';
  const src  = `${base}/${encodeURIComponent(username)}/${encodeURIComponent(filename)}?inline=true`;

  const overlay = document.getElementById('vid-modal-overlay');
  const videoEl = document.getElementById('vid-modal-player');
  document.getElementById('vid-modal-title').textContent = filename;

  if (_plyrInstance) { _plyrInstance.destroy(); _plyrInstance = null; }
  videoEl.src = '';
  videoEl.load();

  _plyrInstance = new Plyr(videoEl, {
    controls: ['play-large','play','progress','current-time','mute','volume','fullscreen'],
    keyboard: { global: false },
  });
  _plyrInstance.source = {
    type: 'video',
    sources: [{ src, type: 'video/mp4' }],
  };

  overlay.classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closeVideoModal(e) {
  if (e && e.target !== document.getElementById('vid-modal-overlay')) return;
  document.getElementById('vid-modal-overlay').classList.remove('open');
  document.body.style.overflow = '';
  if (_plyrInstance) { _plyrInstance.pause(); }
}

document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    document.getElementById('vid-modal-overlay').classList.remove('open');
    document.body.style.overflow = '';
    if (_plyrInstance) _plyrInstance.pause();
  }
});

// ── Init + auto-refresh ───────────────────────────────────────────────────────
loadWatchlist();
loadStats();
loadYTWatchlist();
loadYTStats();
pollLiveStatus();
pollYTLiveStatus();

// refresh watchlist every 15s when on watchlist panel
setInterval(() => {
  const active = document.querySelector('.panel.active');
  if (active && active.id === 'panel-watchlist') {
    loadWatchlist();
    loadStats();
    loadYTWatchlist();
    loadYTStats();
  }
}, 15000);

// poll live status every 60s
setInterval(() => {
  const active = document.querySelector('.panel.active');
  if (active && active.id === 'panel-watchlist') {
    pollLiveStatus();
    pollYTLiveStatus();
  }
}, 60000);