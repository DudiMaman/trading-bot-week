import csv
import datetime as dt
from pathlib import Path
from collections import defaultdict

# קבצי הלוג של הבוט
TRADES_PATH = Path("bot/logs/trades.csv")
EQUITY_PATH = Path("bot/logs/equity_curve.csv")

# הנחת עמלה: 0.1% לכל צד (כניסה + יציאה = ~0.2%)
FEE_RATE = 0.001

# חלון גלגול לניתוח קצר טווח
HOURS_WINDOW = 48


def parse_time(s: str):
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def load_trades():
    if not TRADES_PATH.exists():
        print(f"⚠️ trades.csv not found at {TRADES_PATH}")
        return []

    rows = []
    with TRADES_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r.get("time"):
                continue
            rows.append(r)
    return rows


def equity_stats(since=None):
    if not EQUITY_PATH.exists():
        return None

    points = []
    with EQUITY_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = parse_time(row.get("time", ""))
            if not t:
                continue
            if since and t < since:
                continue
            e = safe_float(row.get("equity"))
            points.append((t, e))

    if not points:
        return None

    points.sort(key=lambda x: x[0])
    start_eq = points[0][1]
    end_eq = points[-1][1]

    max_eq = start_eq
    max_dd = 0.0
    for _, e in points:
        if e > max_eq:
            max_eq = e
        dd = max_eq - e
        if dd > max_dd:
            max_dd = dd

    return {
        "start": start_eq,
        "end": end_eq,
        "delta": end_eq - start_eq,
        "max_dd": max_dd,
    }


def summarize(trades, label: str):
    print(f"📊 FEE-AWARE PERFORMANCE – {label}")
    print(f"Total log rows in trades.csv: {len(trades)}\n")

    if not trades:
        print("⚠️ No trades in this window.\n")
        return

    enter_trades = [r for r in trades if r.get("type") == "ENTER"]
    exit_trades = [r for r in trades if r.get("type") in ("TP1", "TP2", "SL", "TIME")]

    def pnl_of(r):
        return safe_float(r.get("pnl"))

    def notional_of(r):
        # נומינלי בקירוב: |price * qty|
        return abs(safe_float(r.get("price")) * safe_float(r.get("qty")))

    gross_pnl = sum(pnl_of(r) for r in exit_trades)
    total_notional = sum(notional_of(r) for r in trades)
    est_fees = total_notional * FEE_RATE * 2.0  # כניסה + יציאה
    net_pnl = gross_pnl - est_fees

    wins = [r for r in exit_trades if pnl_of(r) > 0]
    losses = [r for r in exit_trades if pnl_of(r) < 0]
    winrate = (len(wins) / len(exit_trades) * 100.0) if exit_trades else 0.0

    print("====== GLOBAL SUMMARY ======")
    print(f"ENTER trades: {len(enter_trades)}")
    print(f"Exit trades:  {len(exit_trades)}")
    print(f"Gross PnL (exits only): {gross_pnl:.4f} USDT")
    print(f"Total notional volume:  {total_notional:.2f} USDT")
    print(f"Assumed fee rate per side: {FEE_RATE * 100:.3f}%")
    print(f"Estimated total fees:     {est_fees:.4f} USDT")
    print(f"Net PnL (gross - fees):   {net_pnl:.4f} USDT")
    print(f"Wins: {len(wins)}, Losses: {len(losses)}, Winrate: {winrate:.2f}%\n")

    # ----- PnL לפי סוג יציאה -----
    from collections import defaultdict as dd
    pnl_by_type = dd(float)
    count_by_type = dd(int)

    for r in exit_trades:
        t = r.get("type")
        pnl = pnl_of(r)
        pnl_by_type[t] += pnl
        count_by_type[t] += 1

    print("PnL by exit type:")
    for t in ("TP1", "TP2", "SL", "TIME"):
        if count_by_type[t]:
            print(f"  {t:4}: {pnl_by_type[t]:7.4f} USDT over {count_by_type[t]} trades")
    print()

    # ----- ביצועים לפי סימבול -----
    sym_stats = dd(lambda: {"trades": 0, "wins": 0, "losses": 0, "gross": 0.0})
    for r in exit_trades:
        sym = r.get("symbol", "?")
        pnl = pnl_of(r)
        s = sym_stats[sym]
        s["trades"] += 1
        s["gross"] += pnl
        if pnl > 0:
            s["wins"] += 1
        elif pnl < 0:
            s["losses"] += 1

    if sym_stats:
        print("====== PERFORMANCE BY SYMBOL ======")
        print(
            f"{'symbol':15} {'trades':>6} {'wins':>6} "
            f"{'losses':>7} {'winrate%':>9} {'gross_pnl':>10} {'avg_pnl':>8}"
        )
        for sym, s in sorted(sym_stats.items(), key=lambda kv: kv[0]):
            tr = s["trades"]
            wr = (s["wins"] / tr * 100.0) if tr else 0.0
            avg = s["gross"] / tr if tr else 0.0
            print(
                f"{sym:15} {tr:6d} {s['wins']:6d} {s['losses']:7d} "
                f"{wr:9.2f} {s['gross']:10.2f} {avg:8.2f}"
            )
        print()

        sorted_syms = sorted(
            sym_stats.items(), key=lambda kv: kv[1]["gross"], reverse=True
        )

        print("Top 5 symbols by gross PnL:")
        for sym, s in sorted_syms[:5]:
            tr = s["trades"]
            wr = (s["wins"] / tr * 100.0) if tr else 0.0
            print(
                f"  {sym:15}  PnL={s['gross']:.2f}  "
                f"trades={tr}  winrate={wr:.1f}%"
            )
        print()

        print("Bottom 5 symbols by gross PnL:")
        for sym, s in sorted_syms[-5:]:
            tr = s["trades"]
            wr = (s["wins"] / tr * 100.0) if tr else 0.0
            print(
                f"  {sym:15}  PnL={s['gross']:.2f}  "
                f"trades={tr}  winrate={wr:.1f}%"
            )
        print()

    # ----- ביצועים לפי כיוון (לונג/שורט) -----
    side_stats = dd(lambda: {"trades": 0, "wins": 0, "losses": 0, "gross": 0.0, "volume": 0.0})
    for r in exit_trades:
        side = r.get("side", "?")
        pnl = pnl_of(r)
        notional = notional_of(r)
        s = side_stats[side]
        s["trades"] += 1
        s["gross"] += pnl
        s["volume"] += notional
        if pnl > 0:
            s["wins"] += 1
        elif pnl < 0:
            s["losses"] += 1

    if side_stats:
        print("====== PERFORMANCE BY SIDE (LONG/SHORT) ======")
        print(
            f"{'side':6} {'trades':>6} {'wins':>6} {'losses':>7} "
            f"{'winrate%':>9} {'volume':>10} {'gross_pnl':>10} {'avg_pnl':>8}"
        )
        for side, s in side_stats.items():
            tr = s["trades"]
            wr = (s["wins"] / tr * 100.0) if tr else 0.0
            avg = s["gross"] / tr if tr else 0.0
            print(
                f"{side:6} {tr:6d} {s['wins']:6d} {s['losses']:7d} "
                f"{wr:9.2f} {s['volume']:10.2f} {s['gross']:10.2f} {avg:8.2f}"
            )
        print()

    # ----- עקומת Equity -----
    print("====== EQUITY CURVE (approx) ======")
    stats = equity_stats()
    if stats:
        print(f"Start equity: {stats['start']:.2f} USDT")
        print(f"End equity:   {stats['end']:.2f} USDT")
        print(f"Delta:        {stats['delta']:.2f} USDT")
        print(f"Max drawdown (approx): {stats['max_dd']:.2f} USDT")
    else:
        print("No equity points available.")

    print("\n" + "=" * 60 + "\n")


def main():
    trades = load_trades()
    now = dt.datetime.now(dt.timezone.utc)
    since_48 = now - dt.timedelta(hours=HOURS_WINDOW)

    # 1) דוח על כל ההיסטוריה
    summarize(trades, "FULL HISTORY")

    # 2) דוח גלגול ל-48 שעות אחרונות
    window_trades = [
        r
        for r in trades
        if (parse_time(r.get("time", "")) or now) >= since_48
    ]
    summarize(window_trades, f"LAST {HOURS_WINDOW}H")


if __name__ == "__main__":
    main()
