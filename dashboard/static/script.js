// static/script.js

const tzKey = 'tz_pref';
const DEFAULT_TZ = 'Asia/Jerusalem';

function getTZ() {
  return localStorage.getItem(tzKey) || DEFAULT_TZ;
}

function setTZ(val) {
  localStorage.setItem(tzKey, val);
}

function onTZChange(select) {
  setTZ(select.value);
  loadData();
}

function toLocalString(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return String(iso);
  const fmt = new Intl.DateTimeFormat('he-IL', {
    timeZone: getTZ(),
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  const p = Object.fromEntries(fmt.formatToParts(d).map(x => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

function getTime(r) {
  return r.time || r.timestamp || r.datetime || r.ts || r.date || null;
}

function paintTrades(rows) {
  const tbody = document.querySelector('#trades-table tbody');
  tbody.innerHTML = '';
  if (!rows || !rows.length) return;

  for (const r of rows) {
    const tr = document.createElement('tr');
    const fields = [
      r.equity ?? '—',
      r.pnl ?? '—',
      r.qty ?? '—',
      r.price ?? '—',
      r.type ?? '—',
      r.side ?? '—',
      r.symbol ?? '—',
      toLocalString(getTime(r))
    ];
    for (const [i, val] of fields.entries()) {
      const td = document.createElement('td');
      td.textContent = val;
      if (i === 1 || i === 5) {
        const v = String(val).toUpperCase();
        if (i === 1) td.classList.add(+val >= 0 ? 'pos' : 'neg');
        if (i === 5 && v === 'BUY' || v === 'LONG') td.classList.add('pos');
        if (i === 5 && v === 'SELL' || v === 'SHORT') td.classList.add('neg');
      }
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

async function loadData() {
  const res = await fetch('/data');
  const data = await res.json();
  document.getElementById('log-dir').textContent = data.log_dir || '—';
  document.getElementById('last-refresh').textContent = toLocalString(data.now_utc || new Date().toISOString());
  paintTrades(data.trades || []);
}

function downloadCSV() {
  location.href = '/export/trades.csv';
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('tz-select').value = getTZ();
  loadData();
  setInterval(loadData, 15000);
});
