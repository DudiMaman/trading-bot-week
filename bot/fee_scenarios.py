# bot/fee_scenarios.py
import csv
import datetime as dt
from pathlib import Path

TRADES_PATH = Path("bot/logs/trades.csv")

EXIT_TYPES = ("TP1", "TP2", "SL", "TIME")
FEE_MODELS = {
    "Bybit_0.10% per side": 0.0010,
    "MEXC_taker_0.04% per side": 0.0004,
    "MEXC_50% maker 50% taker (0.025% per side)": 0.00025,
}

def parse_time(s: str):
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def load_rows():
    if not TRADES_PATH.exists():
        print(f"⚠️ trades.csv not found at {TRADES_PATH}")
        return []
    rows = []
    with TRADES_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def in_window(rows, days=None):
    if days is None:
        return rows
    now = dt.datetime.now(dt.timezone.utc)
    since = now - dt.timedelta(days=days)
    filtered = []
    for r in rows:
        t = parse_time(r.get("time", ""))
        if not t:
            continue
        if t >= since:
            filtered.append(r)
    return filtered

def float_or_zero(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def summarize(rows, fee_rate_side: float):
    """
    fee_rate_side – אחוז עמלה לכל צד, לדוגמה 0.001 = 0.10%
    """
    # נפח מסחר – כל השורות שהן ENTER/TP/SL/TIME
    total_volume = 0.0
    for r in rows:
        if r.get("type") in ("ENTER",) + EXIT_TYPES:
            price = float_or_zero(r.get("price"))
            qty = float_or_zero(r.get("qty"))
            total_volume += abs(price * qty)

    # רווח/הפסד גולמי – רק יציאות
    exit_trades = [r for r in rows if r.get("type") in EXIT_TYPES]
    gross_pnl = sum(float_or_zero(r.get("pnl")) for r in exit_trades)

    est_fees = total_volume * fee_rate_side
    net_pnl = gross_pnl - est_fees

    return {
        "num_rows": len(rows),
        "num_exits": len(exit_trades),
        "volume": total_volume,
        "gross_pnl": gross_pnl,
        "est_fees": est_fees,
        "net_pnl": net_pnl,
    }

def print_block(title, rows):
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)
    if not rows:
        print("⚠️ No trades in this window.")
        return

    print(f"Total log rows in window: {len(rows)}")
    # מודל בסיס – Bybit
    base = summarize(rows, FEE_MODELS["Bybit_0.10% per side"])
    print()
    print("Base stats (Bybit 0.10% per side):")
    print(f"  Exit trades:         {base['num_exits']}")
    print(f"  Gross PnL:           {base['gross_pnl']:.4f} USDT")
    print(f"  Total notional vol.: {base['volume']:.2f} USDT")
    print(f"  Fees (Bybit):        {base['est_fees']:.4f} USDT")
    print(f"  Net PnL:             {base['net_pnl']:.4f} USDT")
    print()

    for name, fee_rate in FEE_MODELS.items():
        stats = summarize(rows, fee_rate)
        print(f"---- {name} ----")
        print(f"  Assumed fee/side:    {fee_rate * 100:.3f}%")
        print(f"  Exit trades:         {stats['num_exits']}")
        print(f"  Gross PnL:           {stats['gross_pnl']:.4f} USDT")
        print(f"  Total notional vol.: {stats['volume']:.2f} USDT")
        print(f"  Est. total fees:     {stats['est_fees']:.4f} USDT")
        print(f"  Net PnL:             {stats['net_pnl']:.4f} USDT")
        print()

def main():
    rows = load_rows()
    if not rows:
        return

    # FULL HISTORY
    print_block("FEE SCENARIOS – FULL HISTORY", rows)

    # LAST 14 DAYS
    rows_14d = in_window(rows, days=14)
    print_block("FEE SCENARIOS – LAST 14 DAYS", rows_14d)

if __name__ == "__main__":
    main()
