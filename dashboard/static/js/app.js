console.log("✅ app.js loaded from dashboard/static/js/");
// app.js — Dynamic dashboard view with timezone selection support
const TIMEZONES = {
  'Asia/Jerusalem': 'IL time',
  'UTC': 'UTC'
};

let currentTz = 'Asia/Jerusalem';
const tzSelect   = document.getElementById('tz-select');
const statusEl   = document.getElementById('status-badge');
const overrideEl = document.getElementById('override-badge');
const lrEl       = document.getElementById('last-refresh');
const logDirEl   = document.getElementById('log-dir');
const downloadCSV= document.getElementById('download-csv');

// Populate timezone selector options
for (const [tz, label] of Object.entries(TIMEZONES)) {
  const opt = document.createElement('option');
  opt.value = tz;
  opt.textContent = label;
  if (tz === currentTz) opt.selected = true;
  tzSelect.appendChild(opt);
}

// Handle timezone selection change
tzSelect.addEventListener('change', () => {
  currentTz = tzSelect.value;
  loadData().catch(console.error);
});

// Helper: format a date/time string or timestamp to the selected timezone
function toTzString(isoLike) {
  if (!isoLike) return '—';
  const d = new Date(isoLike);
  if (isNaN(d.getTime())) return String(isoLike);
  const fmt = new Intl.DateTimeFormat('he-IL', {
    timeZone: currentTz,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false
  });
  const parts = fmt.formatToParts(d).reduce((acc, part) => {
    acc[part.type] = part.value;
    return acc;
  }, {});
  // Construct a "YYYY-MM-DD HH:MM:SS" format string
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

// Fetch and display the latest data
async function loadData() {
  // Indicate loading status
  statusEl.textContent = 'Loading…';
  statusEl.className = 'badge badge-muted';

  // Fetch the main data (trades, equity, etc.)
  const res = await fetch('/data');
  const data = await res.json();

  // Update last refresh time display
  const serverNow = data.now_utc || data.now || data.server_time || new Date().toISOString();
  lrEl.textContent = toTzString(serverNow);

  // Update log directory display
  logDirEl.textContent = data.log_dir || '—';

  // Update override status badge (if the bot is in manual override mode)
  const override = data.override;
  overrideEl.style.display = override ? 'inline-block' : 'none';

  // Update status badge (e.g., Running/Stopped)
  const status = data.status;
  statusEl.textContent = status ? status : '—';
  statusEl.className = 'badge ' + (status === 'Running' ? 'badge-green' : (status === 'Stopped' ? 'badge-red' : 'badge-muted'));

  // Populate trades table
  const trades = data.trades || [];
  const tbody = document.querySelector('#trades-table tbody');
  tbody.innerHTML = '';  // clear existing rows
  if (trades.length === 0) {
    // Show "no trades" message if applicable
    document.getElementById('empty-trades').style.display = 'block';
  } else {
    document.getElementById('empty-trades').style.display = 'none';
    for (const trade of trades) {
      const row = document.createElement('tr');
      // Time (formatted to selected timezone)
      const timeCell = document.createElement('td');
      timeCell.textContent = toTzString(trade.time || trade.timestamp || trade.datetime);
      row.appendChild(timeCell);
      // Symbol
      const symbolCell = document.createElement('td');
      symbolCell.textContent = trade.symbol || '—';
      row.appendChild(symbolCell);
      // Price
      const priceCell = document.createElement('td');
      priceCell.textContent = trade.price !== undefined ? trade.price : '—';
      row.appendChild(priceCell);
      // Quantity
      const qtyCell = document.createElement('td');
      qtyCell.textContent = trade.qty !== undefined ? trade.qty : '—';
      row.appendChild(qtyCell);
      // PnL
      const pnlCell = document.createElement('td');
      pnlCell.textContent = trade.pnl !== undefined ? trade.pnl : '—';
      row.appendChild(pnlCell);
      // Equity
      const equityCell = document.createElement('td');
      equityCell.textContent = trade.equity !== undefined ? trade.equity : '—';
      row.appendChild(equityCell);

      tbody.appendChild(row);
    }
  }

  // (If there were an equity chart or other time-based displays, they would be updated here similarly using toTzString for timestamps.)

  // Set status badge back to normal if data is loaded
  // (For example, if data provides a timestamp or flag indicating last update success)
}
