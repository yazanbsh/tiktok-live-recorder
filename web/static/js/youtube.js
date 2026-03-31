// ── YouTube Watchlist ─────────────────────────────────────────────────────────

async function loadYTWatchlist() {
  const tbody = document.getElementById('yt-watchlist-tbody');
  if (!tbody) return;
  try {
    const users = await apiFetch('/api/yt/users');
    if (!users.length) {
      tbody.innerHTML = `<tr><td colspan="6">
        <div class="empty-state">
          <div class="empty-icon">▶</div>
          <div class="empty-text">No YouTube channels monitored yet.<br>Click + Add Channel to get started.</div>
        </div></td></tr>`;
      return;
    }
    tbody.innerHTML = users.map(renderYTUserRow).join('');
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="6" style="color:var(--accent);font-family:var(--mono);font-size:11px;padding:18px">${e.message}</td></tr>`;
  }
}

function renderYTUserRow(u) {
  const initials = u.username.slice(0, 2).toUpperCase();
  const statusBadge = renderBadge(u.status || 'idle');
  const lastLive = u.last_seen_live
    ? new Date(u.last_seen_live).toLocaleString()
    : '<span style="color:var(--muted)">Never</span>';

  return `<tr>
    <td>
      <div class="username-cell">
        <div class="avatar" style="background:rgba(255,0,0,0.15);color:#ff4444;">▶</div>
        <div>
          <div class="uname">
            <a href="${u.channel_url}" target="_blank" style="color:inherit;text-decoration:none;">
              <span class="at">@</span>${u.username}
            </a>
            ${u.last_error ? `<span title="${escHtml(u.last_error)}" style="cursor:help;color:var(--accent);font-size:11px;">⚠</span>` : ''}
          </div>
          ${u.file_prefix ? `<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px;">prefix: ${u.file_prefix}</div>` : ''}
        </div>
      </div>
    </td>
    <td>${statusBadge}</td>
    <td><span class="mode-tag">${u.interval}m</span></td>
    <td id="yt-live-${u.username}">${renderYTLiveBadge(u.username)}</td>
    <td style="font-size:12px;color:var(--text2)">${lastLive}</td>
    <td style="font-family:var(--mono);font-size:13px">${u.recordings_count || 0}</td>
    <td>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        <button class="action-btn${u.record !== false ? '' : ''}"
          style="${u.record !== false ? 'border-color:var(--accent);color:var(--accent);' : 'color:var(--muted);'}"
          onclick="toggleYTRecord('${u.username}', ${u.record !== false})"
          title="${u.record !== false ? 'Recording enabled — click to disable' : 'Recording disabled — click to enable'}">
          ${u.record !== false ? '⏺ REC' : '⏺ REC OFF'}
        </button>
        <button class="action-btn" onclick="openEditYTModal('${u.username}', ${u.interval}, '${u.file_prefix || ''}', ${u.record !== false})">✎ Edit</button>
        <button class="action-btn danger" onclick="removeYTUser('${u.username}')">✕ Remove</button>
      </div>
    </td>
  </tr>`;
}

function renderYTLiveBadge(username) {
  return `<span class="checking" id="yt-live-badge-${username}" 
    onclick="recheckYTLive('${username}')" style="cursor:pointer;" title="Click to recheck">
    ··· checking
  </span>`;
}

async function recheckYTLive(username) {
  const el = document.getElementById(`yt-live-badge-${username}`);
  if (el) el.innerHTML = `<span class="checking">··· checking</span>`;
  try {
    const data = await apiFetch(`/api/yt/users/${username}/status`);
    if (el) {
      el.innerHTML = data.is_live
        ? `<span class="live-indicator"><span class="live-dot"></span>LIVE</span>`
        : `<span style="color:var(--muted);font-family:var(--mono);font-size:10px;">OFFLINE</span>`;
    }
  } catch(e) {
    if (el) el.innerHTML = `<span style="color:var(--accent);font-family:var(--mono);font-size:10px;">ERR</span>`;
  }
}

async function pollYTLiveStatus() {
  try {
    const users = await apiFetch('/api/yt/users');
    for (const u of users) {
      recheckYTLive(u.username);
    }
  } catch(e) {}
}

async function removeYTUser(username) {
  if (!confirm(`Remove @${username} from YT watchlist?`)) return;
  try {
    await apiFetch(`/api/yt/users/${username}`, { method: 'DELETE' });
    toast(`@${username} removed from YT watchlist`, 'success');
    loadYTWatchlist();
    loadYTStats();
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

async function toggleYTRecord(username, currentlyEnabled) {
  try {
    await apiFetch(`/api/yt/users/${username}`, {
      method: 'PATCH',
      body: JSON.stringify({ record: !currentlyEnabled }),
    });
    toast(`Recording ${!currentlyEnabled ? 'enabled' : 'disabled'} for @${username}`, 'info');
    loadYTWatchlist();
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

// ── Add YT User modal ─────────────────────────────────────────────────────────

function openAddYTModal() {
  document.getElementById('yt-add-modal').classList.add('open');
  document.getElementById('yt-f-username').focus();
}

function closeAddYTModal() {
  document.getElementById('yt-add-modal').classList.remove('open');
  ['yt-f-username', 'yt-f-prefix'].forEach(id => document.getElementById(id).value = '');
  document.getElementById('yt-f-interval').value = '5';
  document.getElementById('yt-f-record').checked = true;
}

async function submitAddYTUser() {
  const username = document.getElementById('yt-f-username').value.trim().replace(/^@/, '');
  if (!username) { toast('Channel handle is required', 'error'); return; }

  const payload = {
    username,
    interval:    parseInt(document.getElementById('yt-f-interval').value) || 5,
    file_prefix: document.getElementById('yt-f-prefix').value.trim() || null,
    record:      document.getElementById('yt-f-record').checked,
  };

  try {
    await apiFetch('/api/yt/users', { method: 'POST', body: JSON.stringify(payload) });
    toast(`@${username} added to YT watchlist`, 'success');
    closeAddYTModal();
    loadYTWatchlist();
    loadYTStats();
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

document.addEventListener('DOMContentLoaded', () => {
  const inp = document.getElementById('yt-f-username');
  if (inp) inp.addEventListener('keydown', e => { if (e.key === 'Enter') submitAddYTUser(); });
});

// ── YT Stats ──────────────────────────────────────────────────────────────────

async function loadYTStats() {
  try {
    const s = await apiFetch('/api/yt/stats');
    const el = id => document.getElementById(id);
    if (el('yt-stat-total'))     el('yt-stat-total').textContent     = s.total_users;
    if (el('yt-stat-recording')) el('yt-stat-recording').textContent = s.currently_recording;
    if (el('yt-stat-clips'))     el('yt-stat-clips').textContent     = s.total_recordings;
    if (el('yt-stat-disk'))      el('yt-stat-disk').textContent      = s.disk_used_mb + ' MB';
  } catch(e) {}
}

// ── Edit YT User modal ───────────────────────────────────────────────────────

function openEditYTModal(username, interval, filePrefix, record) {
  document.getElementById('yt-edit-username').textContent = '@' + username;
  document.getElementById('yt-edit-username').dataset.username = username;
  document.getElementById('yt-edit-interval').value = interval || 5;
  document.getElementById('yt-edit-prefix').value = filePrefix || '';
  document.getElementById('yt-edit-record').checked = record !== false;
  document.getElementById('yt-edit-modal').classList.add('open');
}

function closeEditYTModal() {
  document.getElementById('yt-edit-modal').classList.remove('open');
}

async function submitEditYTUser() {
  const username = document.getElementById('yt-edit-username').dataset.username;
  const payload = {
    interval:    parseInt(document.getElementById('yt-edit-interval').value) || 5,
    file_prefix: document.getElementById('yt-edit-prefix').value.trim() || null,
    record:      document.getElementById('yt-edit-record').checked,
  };
  try {
    await apiFetch(`/api/yt/users/${username}`, {
      method: 'PATCH',
      body: JSON.stringify(payload),
    });
    toast(`@${username} updated`, 'success');
    closeEditYTModal();
    loadYTWatchlist();
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

// ── YT Recordings ─────────────────────────────────────────────────────────────

async function loadYTRecordings() {
  const grid = document.getElementById('yt-rec-grid');
  if (!grid) return;
  try {
    const files = await apiFetch('/api/yt/recordings');
    if (!files.length) {
      grid.innerHTML = `<div class="empty-state">
        <div class="empty-icon">▶</div>
        <div class="empty-text">No YouTube recordings yet.</div>
      </div>`;
      return;
    }
    const groups = {};
    for (const f of files) {
      if (!groups[f.username]) groups[f.username] = [];
      groups[f.username].push(f);
    }
    let html = '';
    for (const [username, recs] of Object.entries(groups)) {
      const totalMB = recs.reduce((s, f) => s + f.size_mb, 0).toFixed(1);
      const key = 'yt_' + username;
      if (!(key in collapsedSections)) collapsedSections[key] = true;
      const isCollapsed = !!collapsedSections[key];
      const sort = _sectionSort[key] || {key:'date', dir:-1};
      const sorted = _sortFiles([...recs], sort.key, sort.dir);
      html += `<div class="rec-section${isCollapsed ? ' collapsed' : ''}" data-user="${key}">
        <div class="rec-section-header" onclick="toggleSection('${key}')">
          <span class="rec-section-arrow">▼</span>
          <span class="rec-section-title">
            <a href="https://youtube.com/@${username}" target="_blank" onclick="event.stopPropagation()">@${username}</a>
          </span>
          <span class="rec-section-meta">
            <span>${recs.length} clip${recs.length !== 1 ? 's' : ''}</span>
            <span>${totalMB} MB</span>
          </span>
          <span onclick="event.stopPropagation()" style="display:flex;gap:4px;margin-left:8px;">
            ${_sortBtnHtml(key, sort, 'loadYTRecordings')}
          </span>
        </div>
        <div class="rec-section-body">`;
      for (const f of sorted) {
        html += `<div class="rec-card">
          <div class="rec-card-top">
            <a class="rec-filename" href="/api/yt/recordings/${f.username}/${encodeURIComponent(f.filename)}?inline=true"
              target="_blank">${f.filename}</a>
          </div>
          <div class="rec-meta">
            <span>${f.size_mb} MB</span>
            <span>${new Date(f.created_at).toLocaleDateString()}</span>
          </div>
          <div class="rec-actions" onclick="event.stopPropagation()">
            <button class="rec-btn rec-btn-play" onclick="openVideoModal('${f.username}','${f.filename}','yt')">▶ Play</button>
            <button class="rec-btn rec-btn-dl" onclick="ytSingleDownload('${f.username}','${f.filename}')">↓ Download</button>
            <button class="rec-btn rec-btn-del" onclick="ytSingleDelete('${f.username}','${f.filename}')">✕ Delete</button>
          </div>
        </div>`;
      }
      html += `</div></div>`;
    }
    grid.innerHTML = html;
  } catch(e) {
    grid.innerHTML = `<div style="color:var(--accent);font-family:var(--mono);font-size:11px;">${e.message}</div>`;
  }
}

function ytSingleDownload(username, filename) {
  const a = document.createElement('a');
  a.href = `/api/yt/recordings/${encodeURIComponent(username)}/${encodeURIComponent(filename)}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function ytSingleDelete(username, filename) {
  if (!confirm(`Delete ${filename}?`)) return;
  try {
    const res = await apiFetch('/api/yt/recordings', {
      method: 'DELETE',
      body: JSON.stringify({ files: [`${username}/${filename}`] }),
    });
    if (res.deleted?.length) {
      toast(`Deleted ${filename}`, 'success');
      loadYTRecordings();
    } else {
      toast(`Failed: ${res.failed?.[0]?.error || 'unknown error'}`, 'error');
    }
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}