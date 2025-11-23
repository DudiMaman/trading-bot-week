import os
import io
import csv
import json
from typing import Optional
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, send_file, abort, request

# ===== הגדרות כלליות =====
APP_TZ = timezone.utc  # השרת עובד ב-UTC
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# נתיב לוגים של הבוט (ניתן לשנות עם ENV בשם LOG_DIR)
DEFAULT_LOG_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "bot", "logs"))
LOG_DIR = os.getenv("LOG_DIR", DEFAULT_LOG_DIR)

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")
STATE_JSON = os.path.join(LOG_DIR, "bot_state.json")  # חיווי ידני להפעלה/עצירה

app = Flask(__name__, template_folder="templates", static_folder="static")

# ===== DB helpers (psycopg v3 / psycopg2) =====
_PSYCOPG_V3_OK = False
_PSYCOPG2_OK = False
try:
    import psycopg as _psycopg_v3
    _PSYCOPG_V3_OK = True
except Exception:
    _PSYCOPG_V3_OK = False

try:
    import psycopg2 as _psycopg2
    _PSYCOPG2_OK = True
except Exception:
    _PSYCOPG2_OK = False


def _db_available() -> bool:
    return bool(os.getenv("DATABASE_URL")) and (_PSYCOPG_V3_OK or _PSYCOPG2_OK)


def _db_connect():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL missing")
    if _PSYCOPG_V3_OK:
        conn = _psycopg_v3.connect(dsn)
        return conn, "v3"
    if _PSYCOPG2_OK:
        conn = _psycopg2.connect(dsn)
        return conn, "v2"
    raise RuntimeError("psycopg not installed")


def _query_db_trades(
    limit: int = 100,
    connector: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
):
    conn, kind = _db_connect()
    try:
        cur = conn.cursor()
        where = []
        params = []
        if connector:
            where.append("connector = %s")
            params.append(connector)
        if start:
            where.append('"time" >= %s')
            params.append(start)
        if end:
            where.append('"time" <= %s')
            params.append(end)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT time, connector, symbol, type, side, price, qty, pnl, equity
            FROM trades
            {where_sql}
            ORDER BY time DESC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(sql, tuple(params))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _query_db_equity(
    limit: int = 200,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
):
    conn, kind = _db_connect()
    try:
        cur = conn.cursor()
        where = []
        params = []
        if start:
            where.append('"time" >= %s')
            params.append(start)
        if end:
            where.append('"time" <= %s')
            params.append(end)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT time, equity
            FROM equity_curve
            {where_sql}
            ORDER BY time DESC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(sql, tuple(params))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _query_db_live_trades(limit: int = 200, connector: Optional[str] = None):
    """
    שאילתת טבלת live_trades – כל שורה = טרייד מאוחד (כניסה/יציאה).
    מנסה קודם SELECT מפורש עם כל העמודות "העשירות".
    אם יש שגיאה (גרסה ישנה בלי חלק מהעמודות) – נופל ל-SELECT * פשוט.
    """
    conn, kind = _db_connect()
    try:
        cur = conn.cursor()
        where = []
        params = []
        if connector:
            where.append("connector = %s")
            params.append(connector)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        sql_rich = f"""
            SELECT
              id,
              connector,
              symbol,
              side,
              time_entry,
              time_exit,
              entry_price,
              exit_price,
              qty,
              pnl,
              pnl_r,
              max_favorable_excursion_r,
              max_adverse_excursion_r,
              bars
            FROM live_trades
            {where_sql}
            ORDER BY COALESCE(time_exit, time_entry) DESC, id DESC
            LIMIT %s
        """
        params_rich = params + [limit]

        try:
            cur.execute(sql_rich, tuple(params_rich))
        except Exception:
            # גרסת טבלה ישנה – לוקחים הכל כמו שהוא
            sql_fallback = f"""
                SELECT *
                FROM live_trades
                {where_sql}
                ORDER BY id DESC
                LIMIT %s
            """
            cur.execute(sql_fallback, tuple(params_rich))

        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _current_status_db(quiet_sec: Optional[int] = None):
    """סטטוס לפי DB (equity/trades)"""
    if not _db_available():
        return {"status": "STOPPED", "last_update": None, "age_sec": None, "source": "db"}

    if quiet_sec is None:
        quiet_sec = int((os.getenv("DASH_QUIET_SEC") or "900").strip())
    now_utc = datetime.now(APP_TZ)

    last_ts = None
    # שאיבה מה-DB (כללי: מקס זמן משתי הטבלאות)
    try:
        eq = _query_db_equity(limit=1)
        if eq:
            t = eq[0]["time"]
            if (last_ts is None) or (t > last_ts):
                last_ts = t
    except Exception:
        pass
    try:
        tr = _query_db_trades(limit=1)
        if tr:
            t = tr[0]["time"]
            if (last_ts is None) or (t > last_ts):
                last_ts = t
    except Exception:
        pass

    if not last_ts:
        return {"status": "STOPPED", "last_update": None, "age_sec": None, "source": "db"}

    age = (now_utc - last_ts).total_seconds()
    return {
        "status": "RUNNING" if age <= quiet_sec else "STOPPED",
        "last_update": last_ts.astimezone(APP_TZ).isoformat(),
        "age_sec": int(age),
        "source": "db",
    }


# ===== אחסון סטטוס ידני (Play/Pause עדין) =====
def _read_state():
    try:
        with open(STATE_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"manual_status": None, "updated_at": None}


def _write_state(obj):
    try:
        with open(STATE_JSON, "w", encoding="utf-8") as f:
            json.dump(obj, f)
    except Exception:
        pass


# ===== יצירת תיקיות/קבצים חסרים אוטומטית =====
def _ensure_logs_and_headers():
    os.makedirs(LOG_DIR, exist_ok=True)
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(
                ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"]
            )
    if not os.path.exists(EQUITY_CSV):
        with open(EQUITY_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["time", "equity"])
    if not os.path.exists(STATE_JSON):
        _write_state({"manual_status": None, "updated_at": None})


_ensure_logs_and_headers()

# ===== עזרי זמן =====
_IL_TZ_NAMES = ["Asia/Jerusalem", "Israel"]  # לא משתמשים בפועל בספריות tz, רק לתיעוד


def _to_dt(ts: str) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    ts = ts.strip()
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(ts, fmt).replace(tzinfo=APP_TZ)
            except Exception:
                continue
    return None


def _last_timestamp(row: dict) -> Optional[datetime]:
    if not isinstance(row, dict):
        return None
    # מעדכן: קודם time_exit / time_entry ואז time רגיל
    for key in ("time_exit", "time_entry", "time", "timestamp", "ts", "datetime", "date"):
        if key in row and row[key]:
            return _to_dt(row[key]) if isinstance(row[key], str) else row[key]
    return None


def _utc_to_il_iso(dt_utc: datetime) -> str:
    if dt_utc is None:
        return ""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=APP_TZ)
    # פשטני: אפריל–אוקטובר +03, אחרת +02
    month = dt_utc.month
    il_offset_hours = 3 if 4 <= month <= 10 else 2
    il_dt = dt_utc + timedelta(hours=il_offset_hours)
    return il_dt.replace(tzinfo=None).isoformat(timespec="seconds") + f"+{il_offset_hours:02d}:00"


# ===== עזרי קבצים =====
def _read_csv(path, limit=None):
    rows = []
    if not os.path.exists(path):
        return rows
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row:
                    rows.append({(k.strip() if isinstance(k, str) else k): v for k, v in row.items()})
    except Exception:
        return []
    if limit:
        try:
            limit = int(limit)
        except Exception:
            limit = None
    return rows[-limit:] if limit else rows


def _compute_range_from_query():
    now = datetime.now(APP_TZ)
    rng = (request.args.get("range") or "").lower().strip()
    p_from = request.args.get("from")
    p_to = request.args.get("to")
    if p_from or p_to:
        start = _to_dt(p_from) if p_from else None
        end = _to_dt(p_to) if p_to else None
        return start, end, "custom"
    if rng in {"last_1h", "1h"}:
        return now - timedelta(hours=1), now, "last_1h"
    if rng in {"last_24h", "24h", "1d"}:
        return now - timedelta(days=1), now, "last_24h"
    if rng in {"last_7d", "7d"}:
        return now - timedelta(days=7), now, "last_7d"
    if rng in {"last_30d", "30d"}:
        return now - timedelta(days=30), now, "last_30d"
    if rng in {"last_90d", "90d"}:
        return now - timedelta(days=90), now, "last_90d"
    return None, None, "all"


def _within_range(ts: Optional[datetime], start: Optional[datetime], end: Optional[datetime]) -> bool:
    if ts is None:
        return False if (start or end) else True
    if start and ts < start:
        return False
    if end and ts > end:
        return False
    return True


def _filter_rows_by_time(rows, start, end):
    out = []
    for r in rows:
        ts = _last_timestamp(r)
        if _within_range(ts, start, end):
            out.append(r)
    return out


# ===== לוגיקת סטטוס (CSV בסיסי) =====
def _bot_status_csv():
    eq_last = _read_csv(EQUITY_CSV, limit=1)
    now = datetime.now(APP_TZ)
    if not eq_last:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    last_ts = _last_timestamp(eq_last[-1])
    if not last_ts:
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None}
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=APP_TZ)
    age = (now - last_ts).total_seconds()
    status = "RUNNING" if age <= 90 else "STOPPED"
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age)}


# ===== Status מאוחד (מכבד Play/Pause) =====
def _status_unified():
    # Play/Pause ידני גובר
    manual = _read_state()
    manual_status = manual.get("manual_status")
    if manual_status in {"RUNNING", "STOPPED"}:
        return {
            "status": manual_status,
            "last_update": None,
            "age_sec": None,
            "source": "manual",
            "manual_override": True,
        }
    # אחרת: DB > CSV
    if _db_available():
        try:
            st = _current_status_db()
            st["manual_override"] = False
            return st
        except Exception:
            pass
    s = _bot_status_csv()
    return {
        "status": s["status"],
        "last_update": s["last_equity_ts"],
        "age_sec": s["age_sec"],
        "source": "csv",
        "manual_override": False,
    }


# ===== ראוטים =====
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify(_status_unified())


@app.route("/api/trades_db")
def api_trades_db():
    if not _db_available():
        return jsonify({"error": "DB not available"}), 503
    # פרמטרים
    try:
        limit = int(request.args.get("limit", "500"))
    except Exception:
        limit = 500
    connector = (request.args.get("connector") or "").strip()
    if connector == "":
        # ברירת מחדל – להציג מציאות חיה: bybit (לא testnet)
        connector = "bybit"

    start, end, _ = _compute_range_from_query()
    rows = _query_db_trades(limit=limit, connector=connector, start=start, end=end)

    # הוספת time_il לתצוגה
    out = []
    for r in rows:
        rr = dict(r)
        ts = _last_timestamp(r)
        rr["time_il"] = _utc_to_il_iso(ts) if ts else ""
        out.append(rr)
    return jsonify(out)


@app.route("/api/equity_db")
def api_equity_db():
    if not _db_available():
        return jsonify({"error": "DB not available"}), 503
    try:
        limit = int(request.args.get("limit", "500"))
    except Exception:
        limit = 500
    start, end, _ = _compute_range_from_query()
    rows = _query_db_equity(limit=limit, start=start, end=end)
    # chronological asc לגרפים
    rows = list(reversed(rows))
    out = []
    for r in rows:
        rr = dict(r)
        ts = _last_timestamp(r)
        rr["time_il"] = _utc_to_il_iso(ts) if ts else ""
        out.append(rr)
    return jsonify(out)


@app.route("/api/live_trades_db")
def api_live_trades_db():
    """
    API חדש לדשבורד הראשי – מחזיר רשימת טריידים מאוחדים מטבלת live_trades.
    כל אובייקט כולל גם time_entry_il / time_exit_il לפי שעון ישראל.
    """
    if not _db_available():
        return jsonify({"error": "DB not available"}), 503

    try:
        limit = int(request.args.get("limit", "300"))
    except Exception:
        limit = 300

    connector = (request.args.get("connector") or "").strip()
    if connector == "":
        connector = "bybit"

    rows = _query_db_live_trades(limit=limit, connector=connector)

    out = []
    for r in rows:
        rr = dict(r)

        # המרה לדפוס ISO אם הגיעו כ-datetime
        for key in ("time_entry", "time_exit"):
            if key in rr and isinstance(rr[key], datetime):
                rr[key] = rr[key].astimezone(APP_TZ).isoformat()

        # מוסיפים time_entry_il / time_exit_il
        ts_entry = None
        ts_exit = None
        if "time_entry" in rr and rr["time_entry"]:
            ts_entry = _to_dt(str(rr["time_entry"])) if isinstance(rr["time_entry"], str) else rr["time_entry"]
        if "time_exit" in rr and rr["time_exit"]:
            ts_exit = _to_dt(str(rr["time_exit"])) if isinstance(rr["time_exit"], str) else rr["time_exit"]

        rr["time_entry_il"] = _utc_to_il_iso(ts_entry) if ts_entry else ""
        rr["time_exit_il"] = _utc_to_il_iso(ts_exit) if ts_exit else ""

        out.append(rr)

    return jsonify(out)


# ===== Back-compat CSV data (UI ישן) =====
@app.route("/data")
def data():
    """
    JSON עם נתוני trades & equity מ-CSV + זמן ישראל, לשמירת תאימות אחורה.
    """
    start, end, label = _compute_range_from_query()
    trades_all = _read_csv(TRADES_CSV)
    equity_all = _read_csv(EQUITY_CSV)
    trades = _filter_rows_by_time(trades_all, start, end)
    equity = _filter_rows_by_time(equity_all, start, end)

    st_csv = _bot_status_csv()
    now_utc = datetime.now(APP_TZ)
    now_il_iso = _utc_to_il_iso(now_utc)

    trades_il = []
    for r in trades:
        rr = dict(r)
        ts = _last_timestamp(r)
        rr["time_il"] = _utc_to_il_iso(ts) if ts else ""
        trades_il.append(rr)

    equity_il = []
    for r in equity:
        rr = dict(r)
        ts = _last_timestamp(r)
        rr["time_il"] = _utc_to_il_iso(ts) if ts else ""
        equity_il.append(rr)

    now_iso_simple = now_utc.strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(
        {
            "status": st_csv["status"],
            "manual_override": False,  # ב-/data הישן לא מערבבים ידני
            "last_equity_ts": st_csv["last_equity_ts"],
            "age_sec": st_csv["age_sec"],
            "now_utc": now_utc.isoformat(),
            "now_utc_simple": now_iso_simple,
            "now_il": now_il_iso,
            "range": {
                "label": label,
                "from": start.isoformat() if start else None,
                "to": end.isoformat() if end else None,
            },
            "trades": trades_il,
            "equity": equity_il,
        }
    )


# ===== יצוא CSV =====
@app.route("/export/trades.csv")
def export_trades():
    start, end, _ = _compute_range_from_query()
    rows = _filter_rows_by_time(_read_csv(TRADES_CSV), start, end)
    if not rows:
        output = io.StringIO()
        csv.writer(output).writerow(["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"])
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="trades.csv",
        )
    headers = list(rows[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="trades.csv",
    )


@app.route("/export/equity_curve.csv")
def export_equity():
    start, end, _ = _compute_range_from_query()
    rows = _filter_rows_by_time(_read_csv(EQUITY_CSV), start, end)
    if not rows:
        output = io.StringIO()
        csv.writer(output).writerow(["time", "equity"])
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name="equity_curve.csv",
        )
    headers = list(rows[0].keys())
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="equity_curve.csv",
    )


# ===== Aliases ובריאות =====
@app.route("/download")
def download_csv_alias():
    if os.path.exists(TRADES_CSV):
        return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")
    abort(404, description="trades.csv not found")


@app.route("/health")
def health():
    st = _status_unified()
    return jsonify(
        {
            "ok": os.path.exists(TRADES_CSV) or os.path.exists(EQUITY_CSV) or _db_available(),
            "has_trades_csv": os.path.exists(TRADES_CSV),
            "has_equity_csv": os.path.exists(EQUITY_CSV),
            "status": st["status"],
            "source": st.get("source"),
            "manual_override": st.get("manual_override", False),
            "last_update": st.get("last_update"),
            "age_sec": st.get("age_sec"),
            "log_dir": LOG_DIR,
            "db_available": _db_available(),
        }
    ), 200


# ===== APIs ל-Play/Pause עדין =====
@app.route("/api/bot/state", methods=["GET"])
def bot_state_get():
    state = _read_state()
    st = _status_unified()
    return jsonify(
        {
            "manual_status": state.get("manual_status"),
            "effective_status": st["status"],
            "updated_at": state.get("updated_at"),
        }
    )


@app.route("/api/bot/start", methods=["POST"])
def bot_state_start():
    now = datetime.now(APP_TZ).isoformat()
    _write_state({"manual_status": "RUNNING", "updated_at": now})
    return jsonify({"ok": True, "manual_status": "RUNNING", "updated_at": now})


@app.route("/api/bot/pause", methods=["POST"])
def bot_state_pause():
    now = datetime.now(APP_TZ).isoformat()
    _write_state({"manual_status": "STOPPED", "updated_at": now})
    return jsonify({"ok": True, "manual_status": "STOPPED", "updated_at": now})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
