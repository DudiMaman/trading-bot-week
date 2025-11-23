import os
import csv
from datetime import datetime

THIS_DIR = os.path.dirname(__file__)
LOG_DIR = os.path.join(THIS_DIR, "logs")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")


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


def analyze(trades):
    if not trades:
        print("⚠️ No trades to analyze.")
        return

    # ניקח רק יציאות כדי לחשב רווחיות: TP1/TP2/SL/TIME
    exit_types = {"TP1", "TP2", "SL", "TIME"}
    exits = [t for t in trades if t["type"] in exit_types]

    total_trades = len(exits)
    if total_trades == 0:
        print("⚠️ No exit trades (TP/SL/TIME) found – can't compute winrate.")
        return

    wins = [t for t in exits if t["pnl"] > 0]
    losses = [t for t in exits if t["pnl"] < 0]

    gross_pnl = sum(t["pnl"] for t in exits)
    avg_pnl = gross_pnl / total_trades if total_trades > 0 else 0.0
    winrate = (len(wins) / total_trades) * 100.0 if total_trades > 0 else 0.0

    # drawdown פשוט על סמך equity ביציאות
    equities = [t["equity"] for t in exits]
    peak = equities[0]
    max_dd = 0.0
    for eq in equities:
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd

    print("====== BOT PERFORMANCE SUMMARY ======")
    print(f"Total exit trades: {total_trades}")
    print(f"Wins: {len(wins)}, Losses: {len(losses)}")
    print(f"Winrate: {winrate:.2f}%")
    print(f"Gross PnL: {gross_pnl:.2f} USDT")
    print(f"Avg PnL per trade: {avg_pnl:.2f} USDT")
    print(f"Max drawdown (approx, based on exit equity): {max_dd:.2f} USDT")

    # פיצול לפי סוג יציאה
    by_type = {}
    for t in exits:
        ttype = t["type"]
        by_type.setdefault(ttype, []).append(t)

    print("\nPnL by exit type:")
    for ttype, arr in by_type.items():
        s = sum(x["pnl"] for x in arr)
        n = len(arr)
        print(f"  {ttype}: {s:.2f} USDT over {n} trades")


def main():
    print(f"📈 Loading trades from: {TRADES_CSV}")
    trades = load_trades(TRADES_CSV)
    analyze(trades)


if __name__ == "__main__":
    main()
