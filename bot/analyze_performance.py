import os
import csv
from datetime import datetime

THIS_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(THIS_DIR, "logs")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")


def load_trades(path: str):
    trades = []
    if not os.path.exists(path):
        print(f"❌ trades.csv not found at: {path}")
        return trades

    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            # מצפים לשדות: time, connector, symbol, type, side, price, qty, pnl, equity
            try:
                row["time"] = datetime.fromisoformat(row["time"])
            except Exception:
                pass
            row["price"] = float(row["price"]) if row.get("price") else 0.0
            row["qty"] = float(row["qty"]) if row.get("qty") else 0.0
            row["pnl"] = float(row["pnl"]) if row.get("pnl") else 0.0
            row["equity"] = float(row["equity"]) if row.get("equity") else 0.0
            trades.append(row)
    return trades


def load_equity_curve(path: str):
    points = []
    if not os.path.exists(path):
        print(f"⚠️ equity_curve.csv not found at: {path}")
        return points

    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                ts = datetime.fromisoformat(row["time"])
            except Exception:
                ts = row.get("time")
            try:
                eq = float(row["equity"])
            except Exception:
                continue
            points.append((ts, eq))
    # ממויין לפי זמן
    points.sort(key=lambda x: x[0] if isinstance(x[0], datetime) else x[0])
    return points


def analyze(trades):
    print("====== BOT PERFORMANCE SUMMARY ======")

    if not trades:
        print("⚠️ No trades in trades.csv.")
        return

    # כל הטריידים
    print(f"Total log rows in trades.csv: {len(trades)}")

    # טריידי כניסה
    enters = [t for t in trades if t["type"] == "ENTER"]
    print(f"ENTER trades: {len(enters)}")

    # ניקח רק יציאות כדי לחשב רווחיות: TP1/TP2/SL/TIME
    exit_types = {"TP1", "TP2", "SL", "TIME"}
    exits = [t for t in trades if t["type"] in exit_types]

    total_trades = len(exits)
    if total_trades == 0:
