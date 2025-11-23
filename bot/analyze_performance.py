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
    # מיון לפי זמן
    points.sort(key=lambda x: x[0] if isinstance(x[0], datetime) else x[0])
    return points


def summarize_by_symbol(exits):
    """
    סיכום ביצועים לפי סימבול:
    - מספר טריידים
    - Winrate
    - PnL כולל
    - PnL ממוצע
    """
    if not exits:
        print("⚠️ No exit trades to summarize by symbol yet.")
        return

    by_symbol = {}
    for t in exits:
        sym = t.get("symbol") or "UNKNOWN"
        by_symbol.setdefault(sym, []).append(t)

    summary = []
    for sym, arr in by_symbol.items():
        n = len(arr)
        wins = sum(1 for x in arr if x["pnl"] > 0)
        losses = sum(1 for x in arr if x["pnl"] < 0)
        gross_pnl = sum(x["pnl"] for x in arr)
        avg_pnl = gross_pnl / n if n > 0 else 0.0
        winrate = (wins / n) * 100.0 if n > 0 else 0.0
        summary.append(
            {
                "symbol": sym,
                "trades": n,
                "wins": wins,
                "losses": losses,
                "winrate": winrate,
                "gross_pnl": gross_pnl,
                "avg_pnl": avg_pnl,
            }
        )

    # מיון מהסימבולים הכי רווחיים להכי מפסידים
    summary.sort(key=lambda x: x["gross_pnl"], reverse=True)

    print("\n====== PERFORMANCE BY SYMBOL ======")
    print("symbol | trades | wins | losses | winrate% | gross_pnl | avg_pnl")
    for row in summary:
        print(
            f"{row['symbol']:10s} "
            f"{row['trades']:6d} "
            f"{row['wins']:5d} "
            f"{row['losses']:7d} "
            f"{row['winrate']:9.2f} "
            f"{row['gross_pnl']:10.2f} "
            f"{row['avg_pnl']:8.2f}"
        )


def analyze(trades):
    print("====== BOT PERFORMANCE SUMMARY ======")

    if not trades:
        print("⚠️ No trades in trades.csv.")
        return

    # כל הרשומות בקובץ
    print(f"Total log rows in trades.csv: {len(trades)}")

    # טריידי כניסה
    enters = [t for t in trades if t.get("type") == "ENTER"]
    print(f"ENTER trades: {len(enters)}")

    # יציאות (TP1/TP2/SL/TIME)
    exit_types = {"TP1", "TP2", "SL", "TIME"}
    exits = [t for t in trades if t.get("type") in exit_types]
    total_trades = len(exits)

    if total_trades == 0:
        print("⚠️ No exit trades (TP/SL/TIME) found – can't compute winrate yet.")
    else:
        wins = [t for t in exits if t["pnl"] > 0]
        losses = [t for t in exits if t["pnl"] < 0]

        gross_pnl = sum(t["pnl"] for t in exits)
        avg_pnl = gross_pnl / total_trades if total_trades > 0 else 0.0
        winrate = (len(wins) / total_trades) * 100.0 if total_trades > 0 else 0.0

        print(f"Exit trades: {total_trades}")
        print(f"Wins: {len(wins)}, Losses: {len(losses)}")
        print(f"Winrate: {winrate:.2f}%")
        print(f"Gross PnL (sum of exits): {gross_pnl:.2f} USDT")
        print(f"Avg PnL per exit trade: {avg_pnl:.2f} USDT")

        # פיצול לפי סוג יציאה
        by_type = {}
        for t in exits:
            ttype = t.get("type")
            by_type.setdefault(ttype, []).append(t)

        print("\nPnL by exit type:")
        for ttype, arr in by_type.items():
            s = sum(x["pnl"] for x in arr)
            n = len(arr)
            print(f"  {ttype}: {s:.2f} USDT over {n} trades")

        # סיכום לפי סימבול
        summarize_by_symbol(exits)

    # גם אם אין יציאות – ננתח equity_curve
    print("\n====== EQUITY CURVE (approx) ======")
    eq_points = load_equity_curve(EQUITY_CSV)
    if not eq_points:
        print("⚠️ No equity points to analyze.")
        return

    start_eq = eq_points[0][1]
    end_eq = eq_points[-1][1]
    delta = end_eq - start_eq

    # max drawdown בסיסי
    peak = eq_points[0][1]
    max_dd = 0.0
    for _, eq in eq_points:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    print(f"Start equity: {start_eq:.2f} USDT")
    print(f"End equity:   {end_eq:.2f} USDT")
    print(f"Delta:        {delta:.2f} USDT")
    print(f"Max drawdown (approx from equity_curve): {max_dd:.2f} USDT")


def main():
    print(f"📈 Loading trades from: {TRADES_CSV}")
    trades = load_trades(TRADES_CSV)
    analyze(trades)


if __name__ == "__main__":
    main()
