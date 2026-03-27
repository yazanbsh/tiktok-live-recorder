// ── Watchlist ────────────────────────────────────────────────────────────────
async function loadWatchlist() {
  const tbody = document.getElementById('watchlist-tbody');
  try {
    const users = await apiFetch('/api/users');
    if (!users.length) {
      tbody.innerHTML = `<tr><td colspan="7">
        <div class="empty-state">
          <div class="empty-icon">◉</div>
          <div class="empty-text">No users in watchlist yet.<br>Click <strong>+ Add User</strong> to start monitoring.</div>
        </div></td></tr>`;
      return;
    }
    tbody.innerHTML = users.map(renderUserRow).join('');
  } catch(e) {
    tbody.innerHTML = `<tr><td colspan="7" style="color:var(--accent);font-family:var(--mono);font-size:11px;padding:20px;">
      Error loading watchlist: ${e.message}</td></tr>`;
  }
}

function renderLiveBadge(username) {
  const s = liveStatus[username];
  if (s === undefined) return `<span class="badge idle" onclick="recheckLive('${username}')" style="cursor:pointer"><span class="badge-dot"></span>CHECKING</span>`;
  if (s === 'checking') return `<span class="badge" style="background:rgba(255,214,10,0.1);color:var(--yellow);border:1px solid rgba(255,214,10,0.2);cursor:pointer" onclick="recheckLive('${username}')"><span class="badge-dot" style="background:var(--yellow)"></span>CHECKING</span>`;
  if (s === true)  return `<span class="badge monitoring" onclick="recheckLive('${username}')" style="cursor:pointer" title="Click to recheck"><span class="badge-dot"></span>LIVE</span>`;
  return `<span class="badge" style="background:rgba(255,45,85,0.08);color:var(--accent);border:1px solid rgba(255,45,85,0.2);cursor:pointer" onclick="recheckLive('${username}')" title="Click to recheck"><span class="badge-dot" style="background:var(--accent)"></span>OFFLINE</span>`;
}

function renderUserRow(u) {
  const initials = u.username.slice(0, 2).toUpperCase();
  const statusBadge = renderBadge(u.status || 'idle');
  const lastLive = u.last_seen_live
    ? new Date(u.last_seen_live).toLocaleString()
    : '<span style="color:var(--muted)">Never</span>';
  const modeTag = `<span class="mode-tag">${u.mode || 'manual'}</span>`;

  return `<tr>
    <td>
      <div class="username-cell">
        <div class="avatar">${initials}</div>
        <div>
          <div class="uname"><a href="https://tiktok.com/@${u.username}" target="_blank" style="color:inherit;text-decoration:none;"><span class="at">@</span>${u.username}</a>${u.last_error ? ` <span title="${u.last_error}" style="cursor:help;color:var(--accent);font-size:11px;">⚠</span>` : ''}</div>
        </div>
      </div>
    </td>
    <td>${statusBadge}</td>
    <td>${modeTag}</td>
    <td>${renderLiveBadge(u.username)}</td>
    <td style="font-size:12px;color:var(--text2)">${lastLive}</td>
    <td style="font-family:var(--mono);font-size:13px">${u.recordings_count || 0}</td>
    <td>
      <div style="display:flex;gap:6px;flex-wrap:wrap">
        ${u.status === 'recording'
          ? `<button class="action-btn" style="border-color:var(--accent);color:var(--accent)" onclick="stopRecording('${u.username}')">■ Stop</button>`
          : u.mode === 'manual' ? `<button class="action-btn primary-btn" onclick="triggerRecord('${u.username}')">▶ Record</button>` : ''
        }
        <button class="action-btn${u.record !== false ? '' : ''}" 
          style="${u.record !== false ? 'border-color:var(--accent);color:var(--accent);' : 'color:var(--muted);'}"
          onclick="toggleRecord('${u.username}', ${u.record !== false})"
          title="${u.record !== false ? 'Recording enabled — click to disable' : 'Recording disabled — click to enable'}">
          ${u.record !== false ? '⏺ REC' : '⏺ REC OFF'}
        </button>
        <button class="action-btn danger" onclick="removeUser('${u.username}')">✕ Remove</button>
      </div>
    </td>
  </tr>`;
}

function renderBadge(status) {
  const labels = { recording:'REC', monitoring:'WATCHING', idle:'IDLE', stopped:'STOPPED', error:'ERROR' };
  const cls = status in labels ? status : 'idle';
  return `<span class="badge ${cls}"><span class="badge-dot"></span>${labels[cls] || status.toUpperCase()}</span>`;
}

// { username: true | false | 'checking' | undefined }
const liveStatus = {};

async function recheckLive(username) {
  liveStatus[username] = 'checking';
  loadWatchlist(); // re-render to show CHECKING state
  try {
    const r = await apiFetch(`/api/users/${username}/status`);
    liveStatus[username] = r.is_live;
    if (r.is_live) toast(`@${username} is <strong>LIVE</strong> right now!`, 'success');
  } catch(e) {
    liveStatus[username] = false;
  }
  loadWatchlist();
}

async function pollLiveStatus() {
  let users = [];
  try { users = await apiFetch('/api/users'); } catch(e) { return; }
  if (!users.length) return;
  // check all users in parallel
  await Promise.allSettled(users.map(async u => {
    liveStatus[u.username] = 'checking';
    try {
      const r = await apiFetch(`/api/users/${u.username}/status`);
      liveStatus[u.username] = r.is_live;
    } catch(e) {
      liveStatus[u.username] = false;
    }
  }));
  loadWatchlist();
}

async function stopRecording(username) {
  try {
    await apiFetch(`/api/users/${username}/stop`, { method: 'POST' });
    toast(`Stop signal sent to @${username}`, 'info');
    setTimeout(loadWatchlist, 1500);
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  }
}

async function triggerRecord(username) {
  try {
    await apiFetch(`/api/users/${username}/record`, { method: 'POST' });
    toast(`Recording triggered for @${username}`, 'success');
    setTimeout(loadWatchlist, 1000);
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  }
}

async function removeUser(username) {
  if (!confirm(`Remove @${username} from watchlist?`)) return;
  try {
    await apiFetch(`/api/users/${username}`, { method: 'DELETE' });
    toast(`@${username} removed`, 'info');
    loadWatchlist();
    loadStats();
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  }
}

// ── Add modal ────────────────────────────────────────────────────────────────
function openAddModal() {
  document.getElementById('add-modal').classList.add('open');
  document.getElementById('f-username').focus();
}
function closeAddModal() {
  document.getElementById('add-modal').classList.remove('open');
}
function onModeChange() {
  const manual = document.getElementById('f-mode').value === 'manual';
  document.getElementById('f-interval-group').style.opacity = manual ? '0.4' : '1';
}
document.getElementById('add-modal').addEventListener('click', e => {
  if (e.target === e.currentTarget) closeAddModal();
});

async function submitAddUser() {
  const username = document.getElementById('f-username').value.trim().replace(/^@/, '');
  if (!username) { toast('Username is required', 'error'); return; }

  const payload = {
    username,
    mode:     document.getElementById('f-mode').value,
    interval: parseInt(document.getElementById('f-interval').value) || 5,
    proxy:    document.getElementById('f-proxy').value.trim() || null,
    duration: parseInt(document.getElementById('f-duration').value) || null,
    bitrate:  document.getElementById('f-bitrate').value.trim() || null,
    record:   document.getElementById('f-record').checked,
  };

  try {
    await apiFetch('/api/users', { method: 'POST', body: JSON.stringify(payload) });
    toast(`@${username} added to watchlist`, 'success');
    closeAddModal();
    // reset form
    ['f-username','f-proxy','f-duration','f-bitrate'].forEach(id => document.getElementById(id).value = '');
    document.getElementById('f-interval').value = '5';
    document.getElementById('f-mode').value = 'automatic';
    document.getElementById('f-record').checked = true;
    loadWatchlist();
    loadStats();
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  }
}

// enter key on modal
document.getElementById('f-username').addEventListener('keydown', e => {
  if (e.key === 'Enter') submitAddUser();
});

async function toggleRecord(username, currentlyEnabled) {
  try {
    await apiFetch(`/api/users/${username}`, {
      method: 'PATCH',
      body: JSON.stringify({ record: !currentlyEnabled }),
    });
    toast(`Recording ${!currentlyEnabled ? 'enabled' : 'disabled'} for @${username}`, 'info');
    loadWatchlist();
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

// ── Recordings ───────────────────────────────────────────────────────────────
// { "username/filename" -> true }
const selectedFiles = {};

function updateBatchBar() {
  const count = Object.keys(selectedFiles).length;
  const bar = document.getElementById('batch-bar');
  document.getElementById('batch-count-num').textContent = count;
  bar.classList.toggle('visible', count > 0);
  // sync select-all checkbox
  const allBoxes = document.querySelectorAll('.rec-checkbox');
  const selectAll = document.getElementById('select-all-recordings');
  if (selectAll) selectAll.checked = allBoxes.length > 0 && allBoxes.length === count;
}

function toggleFile(key, checked) {
  if (checked) selectedFiles[key] = true;
  else delete selectedFiles[key];
  // update card highlight
  document.querySelectorAll(`.rec-card[data-key="${key}"]`).forEach(c => {
    c.classList.toggle('selected', checked);
  });
  updateBatchBar();
}

function toggleSelectAll(cb) {
  document.querySelectorAll('.rec-checkbox').forEach(box => {
    box.checked = cb.checked;
    if (cb.checked) selectedFiles[box.dataset.key] = true;
    else delete selectedFiles[box.dataset.key];
    document.querySelectorAll(`.rec-card[data-key="${box.dataset.key}"]`).forEach(c => {
      c.classList.toggle('selected', cb.checked);
    });
  });
  // update all section checkboxes
  document.querySelectorAll('.rec-section-cb').forEach(scb => {
    scb.checked = cb.checked;
    scb.indeterminate = false;
  });
  updateBatchBar();
}

function clearSelection() {
  Object.keys(selectedFiles).forEach(k => delete selectedFiles[k]);
  document.querySelectorAll('.rec-checkbox').forEach(b => b.checked = false);
  document.querySelectorAll('.rec-card').forEach(c => c.classList.remove('selected'));
  const sa = document.getElementById('select-all-recordings');
  if (sa) sa.checked = false;
  updateBatchBar();
}

async function batchDownload() {
  const keys = Object.keys(selectedFiles);
  if (!keys.length) return;
  for (const key of keys) {
    const [username, filename] = key.split('/');
    const a = document.createElement('a');
    a.href = `/api/recordings/${encodeURIComponent(username)}/${encodeURIComponent(filename)}`;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    await new Promise(r => setTimeout(r, 300)); // slight delay between downloads
  }
}

async function batchDelete() {
  const keys = Object.keys(selectedFiles);
  if (!keys.length) return;
  if (!confirm(`Delete ${keys.length} file${keys.length !== 1 ? 's' : ''}? This cannot be undone.`)) return;
  try {
    const res = await apiFetch('/api/recordings', {
      method: 'DELETE',
      body: JSON.stringify({ files: keys }),
    });
    const d = res.deleted?.length || 0;
    const f = res.failed?.length || 0;
    toast(`Deleted ${d} file${d !== 1 ? 's' : ''}${f ? `, ${f} failed` : ''}`, d > 0 ? 'success' : 'error');
    clearSelection();
    loadRecordings();
    loadStats();
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  }
}

// collapsed state per username (in-memory)
const collapsedSections = {};

// sort state per section key: { key: 'date'|'name'|'size', dir: 1|-1 }
const _sectionSort = {};

function _sortFiles(files, key, dir) {
  return files.sort((a, b) => {
    let va, vb;
    if (key === 'date') { va = a.created_at; vb = b.created_at; }
    else if (key === 'size') { va = a.size_mb; vb = b.size_mb; }
    else { va = a.filename.toLowerCase(); vb = b.filename.toLowerCase(); }
    return va < vb ? -dir : va > vb ? dir : 0;
  });
}

function _sortBtnHtml(sectionKey, current, reloadFn) {
  return ['date','name','size'].map(k => {
    const active = current.key === k;
    const arrow  = active ? (current.dir === -1 ? ' ↓' : ' ↑') : '';
    const style  = active
      ? 'color:var(--text);border-color:var(--text2);'
      : 'color:var(--muted);border-color:var(--border);';
    return `<button class="rec-btn" style="padding:2px 6px;font-size:9px;${style}"
      onclick="_cycleSort('${sectionKey}','${k}','${reloadFn}')">${k}${arrow}</button>`;
  }).join('');
}

function _cycleSort(sectionKey, key, reloadFn) {
  const cur = _sectionSort[sectionKey] || {key:'date', dir:-1};
  if (cur.key === key) {
    _sectionSort[sectionKey] = {key, dir: cur.dir * -1};
  } else {
    _sectionSort[sectionKey] = {key, dir: -1};
  }
  // preserve collapse state then reload
  window[reloadFn]();
}

function toggleSection(username) {
  collapsedSections[username] = !collapsedSections[username];
  const sec = document.querySelector(`.rec-section[data-user="${username}"]`);
  if (sec) sec.classList.toggle('collapsed', !!collapsedSections[username]);
}

function toggleSectionSelect(username, checked) {
  document.querySelectorAll(`.rec-checkbox[data-user="${username}"]`).forEach(box => {
    box.checked = checked;
    if (checked) selectedFiles[box.dataset.key] = true;
    else delete selectedFiles[box.dataset.key];
    document.querySelectorAll(`.rec-card[data-key="${box.dataset.key}"]`).forEach(c => {
      c.classList.toggle('selected', checked);
    });
  });
  updateSectionCheckbox(username);
  updateBatchBar();
}

function updateSectionCheckbox(username) {
  const boxes = document.querySelectorAll(`.rec-checkbox[data-user="${username}"]`);
  const checked = [...boxes].filter(b => b.checked).length;
  const cb = document.querySelector(`.rec-section-cb[data-user="${username}"]`);
  if (!cb) return;
  cb.checked = checked === boxes.length && boxes.length > 0;
  cb.indeterminate = checked > 0 && checked < boxes.length;
}

function playInFilebrowser(username, filename) {
  const host = window.location.hostname;
  const url = `http://${host}:9999/files/recordings/${encodeURIComponent(username)}/${encodeURIComponent(filename)}`;
  window.open(url, '_blank');
}

function singleDownload(username, filename) {
  const a = document.createElement('a');
  a.href = `/api/recordings/${encodeURIComponent(username)}/${encodeURIComponent(filename)}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function singleDelete(username, filename) {
  if (!confirm(`Delete ${filename}?`)) return;
  try {
    const res = await apiFetch('/api/recordings', {
      method: 'DELETE',
      body: JSON.stringify({ files: [`${username}/${filename}`] }),
    });
    if (res.deleted?.length) {
      toast(`Deleted ${filename}`, 'success');
      const key = `${username}/${filename}`;
      delete selectedFiles[key];
      loadRecordings();
      loadStats();
    } else {
      toast(`Failed to delete ${filename}`, 'error');
    }
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

async function loadRecordings() {
  const grid = document.getElementById('rec-grid');
  try {
    const files = await apiFetch('/api/recordings');
    if (!files.length) {
      grid.innerHTML = `<div class="empty-state">
        <div class="empty-icon">▣</div>
        <div class="empty-text">No recordings yet.<br>Recordings will appear here once captured.</div>
      </div>`;
      clearSelection();
      return;
    }
    // group by username
    const groups = {};
    for (const f of files) {
      const u = f.username || 'unknown';
      if (!groups[u]) groups[u] = [];
      groups[u].push(f);
    }
    let html = '';
    for (const [username, recs] of Object.entries(groups)) {
      const totalMB = recs.reduce((s, f) => s + f.size_mb, 0).toFixed(1);
      // collapsed by default unless user explicitly opened it
      if (!(username in collapsedSections)) collapsedSections[username] = true;
      const isCollapsed = !!collapsedSections[username];
      const recSort = _sectionSort['rec_' + username] || {key:'date', dir:-1};
      const sortedRecs = _sortFiles([...recs], recSort.key, recSort.dir);
      html += `<div class="rec-section${isCollapsed ? ' collapsed' : ''}" data-user="${username}">
        <div class="rec-section-header" onclick="toggleSection('${username}')">
          <span class="rec-section-arrow">▼</span>
          <input type="checkbox" class="rec-section-cb" data-user="${username}"
            onclick="event.stopPropagation()"
            onchange="toggleSectionSelect('${username}', this.checked)">
          <span class="rec-section-title">
            <a href="https://tiktok.com/@${username}" target="_blank" onclick="event.stopPropagation()">@${username}</a>
          </span>
          <span class="rec-section-meta">
            <span>${recs.length} clip${recs.length !== 1 ? 's' : ''}</span>
            <span>${totalMB} MB</span>
          </span>
          <span onclick="event.stopPropagation()" style="display:flex;gap:4px;margin-left:8px;">
            ${_sortBtnHtml('rec_' + username, recSort, 'loadRecordings')}
          </span>
        </div>
        <div class="rec-section-body">`;
      for (const f of sortedRecs) {
        const key = `${f.username}/${f.filename}`;
        const isSelected = !!selectedFiles[key];
        html += `<div class="rec-card${isSelected ? ' selected' : ''}" data-key="${key}"
            onclick="toggleFile('${key}', !selectedFiles['${key}']); document.querySelector('.rec-checkbox[data-key=\'${key}\']').checked=!!selectedFiles['${key}']; updateSectionCheckbox('${username}')">
          <div class="rec-card-top">
            <input type="checkbox" class="rec-checkbox" data-key="${key}" data-user="${username}" ${isSelected ? 'checked' : ''}
              onclick="event.stopPropagation()"
              onchange="toggleFile('${key}', this.checked); updateSectionCheckbox('${username}')">
            <a class="rec-filename" href="/api/recordings/${f.username}/${encodeURIComponent(f.filename)}?inline=true"
              target="_blank" onclick="event.stopPropagation()">${f.filename}</a>
          </div>
          <div class="rec-meta">
            <span>${f.size_mb} MB</span>
            <span>${new Date(f.created_at).toLocaleDateString()}</span>
          </div>
          <div class="rec-actions" onclick="event.stopPropagation()">
            <button class="rec-btn rec-btn-play" onclick="openVideoModal('${f.username}','${f.filename}')">▶ Play</button>
            <button class="rec-btn rec-btn-dl"  onclick="singleDownload('${f.username}','${f.filename}')">↓ Download</button>
            <button class="rec-btn rec-btn-del" onclick="singleDelete('${f.username}','${f.filename}')">✕ Delete</button>
          </div>
        </div>`;
      }
      html += `</div></div>`;
    }
    grid.innerHTML = html;
    updateBatchBar();
  } catch(e) {
    grid.innerHTML = `<div style="color:var(--accent);font-family:var(--mono);font-size:11px;">${e.message}</div>`;
  }
}

// ── Downloads ────────────────────────────────────────────────────────────────

// queue rows keyed by item id
const _dlRows = {};
let _sseSource = null;

function _dlStatusCfg(status) {
  return {
    waiting:     { color: 'var(--muted)',   icon: '○', label: 'waiting',     pulse: false },
    processing:  { color: 'var(--yellow)',  icon: '◌', label: 'processing',  pulse: true  },
    downloaded:  { color: 'var(--green)',   icon: '✓', label: 'downloaded',  pulse: false },
    skipped:     { color: 'var(--yellow)',  icon: '↷', label: 'skipped',     pulse: false },
    error:       { color: 'var(--accent)',  icon: '✕', label: 'failed',      pulse: false },
    interrupted: { color: 'var(--muted)',   icon: '⊘', label: 'interrupted', pulse: false },
    removed:     { color: 'var(--muted)',   icon: '—', label: 'removed',     pulse: false },
  }[status] || { color: 'var(--muted)', icon: '○', label: status, pulse: false };
}

function _dlRowHtml(item) {
  const cfg   = _dlStatusCfg(item.status);
  const pulse = cfg.pulse ? 'animation:pulse-dot 1s infinite;' : '';
  const url   = item.url || '';

  let actions = '';
  if (item.status === 'waiting') {
    actions = `<button class="rec-btn rec-btn-del" style="padding:2px 7px;font-size:10px;"
      onclick="dlRemoveQueued('${item.id}')">✕</button>`;
  } else if (item.status === 'interrupted') {
    actions = `<button class="rec-btn rec-btn-play" style="padding:2px 7px;font-size:10px;"
      onclick="dlResume('${item.id}')">▶ Resume</button>`;
  } else if (item.status === 'error') {
    actions = `<button class="rec-btn rec-btn-play" style="padding:2px 7px;font-size:10px;"
      onclick="dlResume('${item.id}')">↺ Retry</button>`;
  }

  const reason = (item.reason && item.status !== 'waiting' && item.status !== 'processing')
    ? `<span style="color:var(--muted);font-size:10px;max-width:200px;overflow:hidden;
         text-overflow:ellipsis;white-space:nowrap;flex-shrink:0;" title="${escHtml(item.reason)}">${escHtml(item.reason)}</span>`
    : '';

  return `<div id="dlrow-${item.id}" style="font-family:var(--mono);font-size:11px;display:flex;
      gap:8px;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);">
    <span style="color:${cfg.color};flex-shrink:0;${pulse}">${cfg.icon}</span>
    <span style="color:var(--text2);word-break:break-all;flex:1;font-size:10px;">${escHtml(url)}</span>
    <span style="color:${cfg.color};flex-shrink:0;white-space:nowrap;">${cfg.label}</span>
    ${reason}
    ${actions}
  </div>`;
}

function _dlUpdateRow(item) {
  _dlRows[item.id] = item;
  const el = document.getElementById(`dlrow-${item.id}`);
  if (el) {
    el.outerHTML = _dlRowHtml(item);
  } else {
    // new item — append to list
    const list = document.getElementById('dl-result-list');
    if (list) {
      list.insertAdjacentHTML('beforeend', _dlRowHtml(item));
      document.getElementById('dl-results').style.display = 'block';
    }
  }
  _dlUpdateSummary();
}

function _dlUpdateSummary() {
  const items  = Object.values(_dlRows);
  const done   = items.filter(i => i.status === 'downloaded').length;
  const skip   = items.filter(i => i.status === 'skipped').length;
  const err    = items.filter(i => i.status === 'error').length;
  const wait   = items.filter(i => i.status === 'waiting').length;
  const proc   = items.filter(i => i.status === 'processing').length;
  const inter  = items.filter(i => i.status === 'interrupted').length;
  const total  = items.length;
  const finished = done + skip + err;

  const pct = total > 0 ? Math.round((finished / total) * 100) : 0;
  const bar = document.getElementById('dl-progress-bar');
  const lbl = document.getElementById('dl-progress-label');
  const barWrap = document.getElementById('dl-progress-bar-wrap');
  if (bar) {
    barWrap.style.display = total > 0 ? 'block' : 'none';
    bar.style.width = pct + '%';
    bar.style.background = err > 0 ? 'var(--accent)' : 'var(--green)';
  }
  if (lbl) lbl.textContent = total > 0 ? `${finished}/${total}` : '';

  const sumEl = document.getElementById('dl-summary');
  if (sumEl && total > 0) {
    sumEl.innerHTML =
      `<span style="color:var(--green)">✓ ${done}</span> &nbsp;` +
      `<span style="color:var(--yellow)">↷ ${skip}</span> &nbsp;` +
      `<span style="color:var(--accent)">✕ ${err}</span>` +
      (wait + proc > 0 ? ` &nbsp;<span style="color:var(--muted)">○ ${wait + proc} pending</span>` : '') +
      (inter > 0 ? ` &nbsp;<span style="color:var(--muted)">⊘ ${inter} interrupted</span>` : '');
  }
}

function _dlConnectSSE() {
  if (_sseSource) { _sseSource.close(); _sseSource = null; }
  const es = new EventSource('/api/tiktok/downloads/stream');
  _sseSource = es;

  es.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'ping') return;

    if (msg.type === 'snapshot') {
      // load full queue state on connect
      msg.items.forEach(item => {
        if (item.status === 'removed') return;
        _dlRows[item.id] = item;
      });
      // render all rows
      const list = document.getElementById('dl-result-list');
      if (list) {
        const activeItems = Object.values(_dlRows).filter(i => i.status !== 'removed');
        if (activeItems.length > 0) {
          list.innerHTML = activeItems.map(_dlRowHtml).join('');
          document.getElementById('dl-results').style.display = 'block';
        }
      }
      _dlUpdateSummary();
      _dlCheckResumeBtn();
      return;
    }

    if (msg.type === 'update') {
      if (msg.status === 'removed') {
        delete _dlRows[msg.id];
        const el = document.getElementById(`dlrow-${msg.id}`);
        if (el) el.remove();
        _dlUpdateSummary();
        return;
      }
      const existing = _dlRows[msg.id] || {};
      _dlUpdateRow({ ...existing, ...msg });
      if (msg.status === 'downloaded') {
        loadDownloadsList();
      }
    }
  };

  es.onerror = () => {
    // reconnect after 3s
    setTimeout(_dlConnectSSE, 3000);
  };
}

// start SSE connection
_dlConnectSSE();

function handleUrlFileDrop(event) {
  event.preventDefault();
  const ta = document.getElementById('dl-urls-input');
  ta.style.borderColor = 'var(--border2)';
  const file = event.dataTransfer.files[0];
  if (!file) return;
  if (!file.name.endsWith('.txt')) {
    toast('Only .txt files are supported', 'error');
    return;
  }
  const reader = new FileReader();
  reader.onload = e => {
    const text = e.target.result.trim();
    ta.value = ta.value ? ta.value + '\n' + text : text;
    toast(`Loaded ${text.split('\n').filter(Boolean).length} lines from ${file.name}`, 'info');
  };
  reader.readAsText(file);
}

async function dlClearByStatus(status) {
  try {
    await apiFetch('/api/tiktok/downloads/queue', {
      method: 'DELETE',
      body: JSON.stringify({ statuses: [status] }),
    });
    // UI update handled by SSE broadcast (status: removed)
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

async function dlResumeQueue() {
  try {
    // just submit empty list — backend will start worker and pick up waiting items
    await apiFetch('/api/tiktok/downloads/resume-queue', { method: 'POST' });
    document.getElementById('dl-resume-queue-btn').style.display = 'none';
    toast('Queue resumed', 'info');
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

function _dlCheckResumeBtn() {
  const hasResumable = Object.values(_dlRows).some(i => i.status === 'waiting' || i.status === 'interrupted');
  const btn = document.getElementById('dl-resume-queue-btn');
  if (btn) btn.style.display = hasResumable ? '' : 'none';
}

async function submitDownloads() {
  const input = document.getElementById('dl-urls-input').value.trim();
  if (!input) { toast('Please enter at least one URL', 'error'); return; }

  const urls = input.split('\n').map(u => u.trim()).filter(Boolean);
  const btn  = document.getElementById('dl-submit-btn');
  btn.disabled = true;

  document.getElementById('dl-urls-input').value = '';

  try {
    const res = await apiFetch('/api/tiktok/downloads', {
      method: 'POST',
      body: JSON.stringify({ urls }),
    });
    if (res.queued > 0) {
      toast(`${res.queued} URL${res.queued !== 1 ? 's' : ''} added to queue`, 'info');
      document.getElementById('dl-resume-queue-btn').style.display = 'none';
    } else {
      toast('All URLs already in queue or duplicate', 'info');
    }
  } catch(e) {
    toast(`Error: ${e.message}`, 'error');
  } finally {
    btn.disabled = false;
  }
}

async function dlRemoveQueued(itemId) {
  try {
    await apiFetch(`/api/tiktok/downloads/queue/${itemId}`, { method: 'DELETE' });
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

async function dlResume(itemId) {
  try {
    await apiFetch(`/api/tiktok/downloads/queue/${itemId}/resume`, { method: 'POST' });
    toast('Re-queued', 'info');
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}

async function loadDownloadsList() {
  const grid = document.getElementById('dl-grid');
  if (!grid) return;
  try {
    const files = await apiFetch('/api/tiktok/downloads');
    if (!files.length) {
      grid.innerHTML = `<div class="empty-state"><div class="empty-icon">▤</div><div class="empty-text">No downloads yet.</div></div>`;
      return;
    }
    const groups = {};
    for (const f of files) {
      if (!groups[f.username]) groups[f.username] = [];
      groups[f.username].push(f);
    }
    let html = '';
    for (const [username, items] of Object.entries(groups)) {
      const totalMB = items.reduce((s, f) => s + f.size_mb, 0).toFixed(1);
      const dlKey = 'dl_' + username;
      if (!(dlKey in collapsedSections)) collapsedSections[dlKey] = true;
      const isDlCollapsed = !!collapsedSections[dlKey];
      const dlSort = _sectionSort[dlKey] || {key:'date', dir:-1};
      const sortedItems = _sortFiles([...items], dlSort.key, dlSort.dir);
      html += `<div class="rec-section${isDlCollapsed ? ' collapsed' : ''}" data-user="${dlKey}">
        <div class="rec-section-header" onclick="toggleSection('${dlKey}')">
          <span class="rec-section-arrow">▼</span>
          <span class="rec-section-title">
            <a href="https://tiktok.com/@${username}" target="_blank" onclick="event.stopPropagation()">@${username}</a>
          </span>
          <span class="rec-section-meta">
            <span>${items.length} file${items.length !== 1 ? 's' : ''}</span>
            <span>${totalMB} MB</span>
          </span>
          <span onclick="event.stopPropagation()" style="display:flex;gap:4px;margin-left:8px;">
            ${_sortBtnHtml(dlKey, dlSort, 'loadDownloadsList')}
          </span>
        </div>
        <div class="rec-section-body">`;
      for (const f of sortedItems) {
        html += `<div class="rec-card">
          <div class="rec-card-top">
            <a class="rec-filename" href="/api/tiktok/downloads/${f.username}/${encodeURIComponent(f.filename)}?inline=true"
              target="_blank">${f.filename}</a>
          </div>
          <div class="rec-meta">
            <span>${f.size_mb} MB</span>
            <span>${new Date(f.created_at).toLocaleDateString()}</span>
          </div>
          <div class="rec-actions" onclick="event.stopPropagation()">
            <button class="rec-btn rec-btn-play" onclick="openVideoModal('${f.username}','${f.filename}','dl')">▶ Play</button>
            <button class="rec-btn rec-btn-dl" onclick="dlSingleDownload('${f.username}','${f.filename}')">↓ Download</button>
            <button class="rec-btn rec-btn-del" onclick="dlSingleDelete('${f.username}','${f.filename}')">✕ Delete</button>
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

function dlSingleDownload(username, filename) {
  const a = document.createElement('a');
  a.href = `/api/tiktok/downloads/${encodeURIComponent(username)}/${encodeURIComponent(filename)}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function dlSingleDelete(username, filename) {
  if (!confirm(`Delete ${filename}?`)) return;
  try {
    const res = await apiFetch('/api/tiktok/downloads', {
      method: 'DELETE',
      body: JSON.stringify({ files: [`${username}/${filename}`] }),
    });
    if (res.deleted?.length) {
      toast(`Deleted ${filename}`, 'success');
      loadDownloadsList();
    } else {
      toast(`Failed: ${res.failed?.[0]?.error || 'unknown error'}`, 'error');
    }
  } catch(e) { toast(`Error: ${e.message}`, 'error'); }
}