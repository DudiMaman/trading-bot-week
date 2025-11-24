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
            try:
                row["price"] = float(row["price"]) if row.get("price") else 0.0
            except Exception:
                row["price"] = 0.0
            try:
                row["qty"] = float(row["qty"]) if row.get("qty") else 0.0
            except Exception:
                row["qty"] = 0.0
            try:
                row["pnl"] = float(row["pnl"]) if row.get("pnl") else 0.0
            except Exception:
                row["pnl"] = 0.0
            try:
                row["equity"] = float(row["equity"]) if row.get("equity") else 0.0
            except Exception:
                row["equity"] = 0.0
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

    points.sort(key=lambda x: x[0] if isinstance(x[0], datetime) else x[0])
    return points


def streaks_from_exits(exits):
    """
    מחשב רצף ניצחונות/הפסדים הכי ארוך ברצף הכרונולוגי.
    """
    if not exits:
        return 0, 0

    # ממיינים לפי זמן אם יש
    sorted_exits = sorted(
        exits,
        key=lambda t: t["time"] if isinstance(t["time"], datetime) else datetime.min,
    )

    max_win_streak = 0
    max_loss_streak = 0
    cur_win = 0
    cur_loss = 0

    for t in sorted_exits:
        pnl = t.get("pnl", 0.0)
        if pnl > 0:
            cur_win += 1
            cur_loss = 0
        elif pnl < 0:
            cur_loss += 1
            cur_win = 0
        else:
            # pnl == 0: שוברים רצף
            cur_win = 0
            cur_loss = 0

        if cur_win > max_win_streak:
            max_win_streak = cur_win
        if cur_loss > max_loss_streak:
            max_loss_streak = cur_loss

    return max_win_streak, max_loss_streak


def summarize_by_symbol(exits):
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

    summary.sort(key=lambda x: x["gross_pnl"], reverse=True)

    print("\n====== PERFORMANCE BY SYMBOL ======")
    print("symbol       trades  wins  losses  winrate%   gross_pnl   avg_pnl")
    for row in summary:
        print(
            f"{row['symbol']:<11s} "
            f"{row['trades']:6d} "
            f"{row['wins']:5d} "
            f"{row['losses']:7d} "
            f"{row['winrate']:9.2f} "
            f"{row['gross_pnl']:11.2f} "
            f"{row['avg_pnl']:9.2f}"
        )


def summarize_by_side(exits):
    if not exits:
        print("⚠️ No exit trades to summarize by side yet.")
        return

    by_side = {}
    for t in exits:
        side = t.get("side") or "unknown"
        by_side.setdefault(side, []).append(t)

    print("\n====== PERFORMANCE BY SIDE (LONG/SHORT) ======")
    print("side    trades  wins  losses  winrate%   gross_pnl   avg_pnl")
    for side, arr in by_side.items():
        n = len(arr)
        wins = sum(1 for x in arr if x["pnl"] > 0)
        losses = sum(1 for x in arr if x["pnl"] < 0)
        gross_pnl = sum(x["pnl"] for x in arr)
        avg_pnl = gross_pnl / n if n > 0 else 0.0
        winrate = (wins / n) * 100.0 if n > 0 else 0.0
        print(
            f"{side:<7s} "
            f"{n:6d} "
            f"{wins:5d} "
            f"{losses:7d} "
            f"{winrate:9.2f} "
            f"{gross_pnl:11.2f} "
            f"{avg_pnl:9.2f}"
        )


def analyze(trades):
    print("====== ADVANCED BOT PERFORMANCE ======")

    if not trades:
        print("⚠️ No trades in trades.csv.")
        return

    print(f"Total log rows in trades.csv: {len(trades)}")

    enters = [t for t in trades if t.get("type") == "ENTER"]
    print(f"ENTER trades: {len(enters)}")

    exit_types = {"TP1", "TP2", "SL", "TIME"}
    exits = [t for t in trades if t.get("type") in exit_types]
    total_exits = len(exits)

    if total_exits == 0:
        print("⚠️ No exit trades (TP/SL/TIME) found – can't compute PnL stats yet.")
    else:
        wins = [t for t in exits if t["pnl"] > 0]
        losses = [t for t in exits if t["pnl"] < 0]

        gross_pnl = sum(t["pnl"] for t in exits)
        avg_pnl = gross_pnl / total_exits if total_exits > 0 else 0.0
        winrate = (len(wins) / total_exits) * 100.0 if total_exits > 0 else 0.0

        print(f"Exit trades: {total_exits}")
        print(f"Wins: {len(wins)}, Losses: {len(losses)}")
        print(f"Winrate: {winrate:.2f}%")
        print(f"Gross PnL (sum of exits): {gross_pnl:.2f} USDT")
        print(f"Avg PnL per exit trade: {avg_pnl:.2f} USDT")

        by_type = {}
        for t in exits:
            ttype = t.get("type")
            by_type.setdefault(ttype, []).append(t)

        print("\nPnL by exit type:")
        for ttype, arr in by_type.items():
            s = sum(x["pnl"] for x in arr)
            n = len(arr)
            print(f"  {ttype}: {s:.2f} USDT over {n} trades")

        max_win_streak, max_loss_streak = streaks_from_exits(exits)
        print(f"\nMax win streak:   {max_win_streak}")
        print(f"Max loss streak:  {max_loss_streak}")

        summarize_by_symbol(exits)
        summarize_by_side(exits)

    print("\n====== EQUITY CURVE (approx) ======")
    eq_points = load_equity_curve(EQUITY_CSV)
    if not eq_points:
        print("⚠️ No equity points to analyze.")
        return

    start_eq = eq_points[0][1]
    end_eq = eq_points[-1][1]
    delta = end_eq - start_eq

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
    print(f"📊 Loading trades from: {TRADES_CSV}")
    trades = load_trades(TRADES_CSV)
    analyze(trades)


if __name__ == "__main__":
    main()
