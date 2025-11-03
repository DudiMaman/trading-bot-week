/* tz-toggle.js
 * Timezone toggle for the existing dark dashboard.
 * Non-invasive: does not change layout/CSS, only adds a small floating control
 * and converts the first column ("Time (UTC)") of the trades table.
 * Default: IL time. Preference is saved in localStorage ("tz_pref": "IL" | "UTC").
 */

(function () {
  const TZ_KEY = "tz_pref";
  const TZ_IL = "IL";
  const TZ_UTC = "UTC";
  const IL_TZ_NAME = "Asia/Jerusalem";

  // ===== Utilities =====
  function getPref() {
    const v = localStorage.getItem(TZ_KEY);
    return v === TZ_UTC ? TZ_UTC : TZ_IL; // default IL
  }
  function setPref(v) {
    localStorage.setItem(TZ_KEY, v);
  }

  function fmtIL(date) {
    // Format to IL using Intl (DST handled automatically)
    const fmt = new Intl.DateTimeFormat("en-GB", {
      timeZone: IL_TZ_NAME,
      year: "numeric",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
      timeZoneName: undefined,
    });
    return fmt.format(date).replace(",", "");
  }

  function parseAsDate(text) {
    // The table shows like: "Mon, 03 Nov 2025 11:18:47 GMT"
    // This is parseable by the Date ctor in most browsers.
    // Fallback: return null if invalid.
    const d = new Date(text);
    return isNaN(d.getTime()) ? null : d;
  }

  // Convert one cell according to tz, preserving original in data-utc
  function convertCell(cell, tz) {
    if (!cell) return;

    // keep original text once
    if (!cell.dataset.utc) {
      const orig = cell.textContent.trim();
      cell.dataset.utc = orig;
    }
    const utcText = cell.dataset.utc;

    if (tz === TZ_UTC) {
      // restore original
      cell.textContent = utcText;
      return;
    }

    // tz === IL
    const d = parseAsDate(utcText);
    if (!d) {
      cell.textContent = utcText;
      return;
    }
    cell.textContent = fmtIL(d) + " IL";
  }

  function convertHeaderTh(th, tz) {
    if (!th) return;
    if (!th.dataset.base) th.dataset.base = th.textContent.trim();
    const base = th.dataset.base;
    // Replace only the "(UTC)" marker if exists, otherwise append
    if (tz === TZ_UTC) {
      th.textContent = base.replace(/\(IL\)|\(UTC\)/g, "").trim() + " (UTC)";
    } else {
      th.textContent = base.replace(/\(IL\)|\(UTC\)/g, "").trim() + " (IL)";
    }
  }

  function convertTable(tz) {
    const table = document.querySelector("#trades-table, table"); // prefer id if exists
    if (!table) return;

    const theadThs = table.querySelectorAll("thead th");
    if (theadThs && theadThs.length) {
      // assume first column is time
      convertHeaderTh(theadThs[0], tz);
    }

    const rows = table.querySelectorAll("tbody tr");
    rows.forEach((tr) => {
      const firstCell = tr.querySelector("td, th"); // first column
      convertCell(firstCell, tz);
    });
  }

  // Observe table changes (auto refresh in your app) and re-apply conversion
  let observer = null;
  function observeTable(tz) {
    const tbody =
      document.querySelector("#trades-table tbody") ||
      document.querySelector("table tbody");
    if (!tbody) return;

    if (observer) observer.disconnect();
    observer = new MutationObserver(() => {
      convertTable(tz);
    });
    observer.observe(tbody, { childList: true, subtree: true });
  }

  // Small floating control (top-right) – non-invasive
  function injectControl(prefTz) {
    if (document.getElementById("tz-toggle")) return;
    const wrap = document.createElement("div");
    wrap.id = "tz-toggle";
    wrap.style.position = "fixed";
    wrap.style.top = "12px";
    wrap.style.right = "12px";
    wrap.style.zIndex = "9999";
    wrap.style.background = "rgba(20,20,30,0.9)";
    wrap.style.border = "1px solid rgba(255,255,255,0.1)";
    wrap.style.borderRadius = "8px";
    wrap.style.padding = "6px 10px";
    wrap.style.font = "12px system-ui, -apple-system, Segoe UI, Roboto, Arial";
    wrap.style.color = "#cfd3dc";
    wrap.style.boxShadow = "0 2px 10px rgba(0,0,0,0.25)";

    const label = document.createElement("span");
    label.textContent = "Timezone:";
    label.style.marginRight = "6px";

    const select = document.createElement("select");
    select.innerHTML = `
      <option value="${TZ_IL}">IL time</option>
      <option value="${TZ_UTC}">UTC</option>
    `;
    select.value = prefTz;
    select.style.background = "#111827";
    select.style.color = "#cfd3dc";
    select.style.border = "1px solid rgba(255,255,255,0.15)";
    select.style.borderRadius = "6px";
    select.style.padding = "4px 6px";

    select.addEventListener("change", () => {
      const tz = select.value === TZ_UTC ? TZ_UTC : TZ_IL;
      setPref(tz);
      convertTable(tz);
      observeTable(tz);
    });

    wrap.appendChild(label);
    wrap.appendChild(select);
    document.body.appendChild(wrap);
  }

  function init() {
    const pref = getPref(); // "IL" | "UTC"
    injectControl(pref);
    convertTable(pref);
    observeTable(pref);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
