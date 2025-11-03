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
# שמות איזור הזמן של ישראל משתנים לפי מערכת; ננסה כמה בצורה בטוחה  
_IL_TZ_NAMES = ["Asia/Jerusalem", "Israel"]  
  
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
    for key in ("time", "timestamp", "ts", "datetime", "date"):  
        if key in row and row[key]:  
            return _to_dt(row[key])  
    return None  
  
def _utc_to_il_iso(dt_utc: datetime) -> str:  
    """  
    מקבל datetime עם tzinfo=UTC או נאיבי (נחשב כ-UTC), ומחזיר ISO עם אופסט ישראל (+02:00/+03:00).  
    לא משתמשים בספריות צד שלישי; לכן נגזור ידנית את האופסט לפי DST ידוע (פשוט: אם שמור tzinfo – נשתמש בו).  
    """  
    if dt_utc is None:  
        return ""  
    if dt_utc.tzinfo is None:  
        dt_utc = dt_utc.replace(tzinfo=APP_TZ)  
  
    # חישוב אופסט ישראל: נשתמש בהבדל מול זמן מקומי של Asia/Jerusalem דרך timestamp  
    # בלי ספריות, נחשב לפי offset עונתי: נובמבר–מרץ ≈ +02, אפריל–אוקטובר ≈ +03 (כללי; מספיק לתצוגה)  
    month = dt_utc.month  
    il_offset_hours = 3 if 4 <= month <= 10 else 2  # פשטני אך יעיל לתצוגה  
    il_dt = dt_utc + timedelta(hours=il_offset_hours)  
    sign = "+"  # ישראל היא UTC+  
    return il_dt.replace(tzinfo=None).isoformat(timespec="seconds") + f"{sign}{il_offset_hours:02d}:00"  
  
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
  
# ===== לוגיקת סטטוס =====  
def _bot_status():  
    state = _read_state()  
    if state.get("manual_status") in {"RUNNING", "STOPPED"}:  
        return {  
            "status": state["manual_status"],  
            "last_equity_ts": None,  
            "age_sec": None,  
            "manual_override": True,  
        }  
    eq_last = _read_csv(EQUITY_CSV, limit=1)  
    now = datetime.now(APP_TZ)  
    if not eq_last:  
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None, "manual_override": False}  
    last_ts = _last_timestamp(eq_last[-1])  
    if not last_ts:  
        return {"status": "STOPPED", "last_equity_ts": None, "age_sec": None, "manual_override": False}  
    if last_ts.tzinfo is None:  
        last_ts = last_ts.replace(tzinfo=APP_TZ)  
    age = (now - last_ts).total_seconds()  
    status = "RUNNING" if age <= 90 else "STOPPED"  
    return {"status": status, "last_equity_ts": last_ts.isoformat(), "age_sec": int(age), "manual_override": False}  
  
@app.route("/")  
def index():  
    return render_template("index.html")  
  
@app.route("/data")  
def data():  
    """  
    מחזיר JSON עם נתוני trades & equity מסוננים לפי טווח.  
    הוספה (לא שוברת UI קיים):  
      - now_il  
      - לכל שורה: time_il (זמן ישראל המומר)  
    """  
    start, end, label = _compute_range_from_query()  
    trades_all = _read_csv(TRADES_CSV)  
    equity_all = _read_csv(EQUITY_CSV)  
    trades = _filter_rows_by_time(trades_all, start, end)  
    equity = _filter_rows_by_time(equity_all, start, end)  
    st = _bot_status()  
  
    # חישובי זמן  
    now_utc = datetime.now(APP_TZ)  
    now_il_iso = _to_il_iso_for_now(now_utc)  
  
    # הוספת time_il לכל שורה (מבלי לשנות השדות המקוריים)  
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
  
    # פורמט פשוט ל-last refresh (UTC) ללא שבר שניות  
    now_iso_simple = now_utc.strftime("%Y-%m-%d %H:%M:%S")  
  
    return jsonify(  
        {  
            "status": st["status"],  
            "manual_override": st.get("manual_override", False),  
            "last_equity_ts": st["last_equity_ts"],  
            "age_sec": st["age_sec"],  
            "now_utc": now_utc.isoformat(),  
            "now_utc_simple": now_iso_simple,  
            "now_il": now_il_iso,  # חדש — זמן ישראל  
            "range": {  
                "label": label,  
                "from": start.isoformat() if start else None,  
                "to": end.isoformat() if end else None,  
            },  
            "trades": trades_il,   # כולל time_il  
            "equity": equity_il,   # כולל time_il  
        }  
    )  
  
def _to_il_iso_for_now(now_utc: datetime) -> str:  
    return _utc_to_il_iso(now_utc)  
  
@app.route("/export/trades.csv")  
def export_trades():  
    start, end, _ = _compute_range_from_query()  
    rows = _filter_rows_by_time(_read_csv(TRADES_CSV), start, end)  
    if not rows:  
        output = io.StringIO()  
        csv.writer(output).writerow(["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"])  
        output.seek(0)  
        return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",  
                         as_attachment=True, download_name="trades.csv")  
    headers = list(rows[0].keys())  
    output = io.StringIO()  
    writer = csv.DictWriter(output, fieldnames=headers)  
    writer.writeheader()  
    writer.writerows(rows)  
    output.seek(0)  
    return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",  
                     as_attachment=True, download_name="trades.csv")  
  
@app.route("/export/equity_curve.csv")  
def export_equity():  
    start, end, _ = _compute_range_from_query()  
    rows = _filter_rows_by_time(_read_csv(EQUITY_CSV), start, end)  
    if not rows:  
        output = io.StringIO()  
        csv.writer(output).writerow(["time", "equity"])  
        output.seek(0)  
        return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",  
                         as_attachment=True, download_name="equity_curve.csv")  
    headers = list(rows[0].keys())  
    output = io.StringIO()  
    writer = csv.DictWriter(output, fieldnames=headers)  
    writer.writeheader()  
    writer.writerows(rows)  
    output.seek(0)  
    return send_file(io.BytesIO(output.getvalue().encode("utf-8")), mimetype="text/csv",  
                     as_attachment=True, download_name="equity_curve.csv")  
  
@app.route("/download")  
def download_csv_alias():  
    if os.path.exists(TRADES_CSV):  
        return send_file(TRADES_CSV, as_attachment=True, download_name="trades.csv")  
    abort(404, description="trades.csv not found")  
  
@app.route("/health")  
def health():  
    st = _bot_status()  
    return jsonify(  
        {  
            "ok": os.path.exists(TRADES_CSV) or os.path.exists(EQUITY_CSV),  
            "has_trades_csv": os.path.exists(TRADES_CSV),  
            "has_equity_csv": os.path.exists(EQUITY_CSV),  
            "status": st["status"],  
            "manual_override": st.get("manual_override", False),  
            "last_equity_ts": st["last_equity_ts"],  
            "age_sec": st["age_sec"],  
            "log_dir": LOG_DIR,  
        }  
    ), 200  
  
# ===== APIs ל-Play/Pause עדין =====  
@app.route("/api/bot/state", methods=["GET"])  
def bot_state_get():  
    state = _read_state()  
    st = _bot_status()  
    return jsonify({"manual_status": state.get("manual_status"), "effective_status": st["status"],  
                    "updated_at": state.get("updated_at")})  
  
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
