# bot/simulate_mexc_fees.py
# ------------------------------------------------------------
# סימולציית עמלות: Bybit vs MEXC על בסיס trades.csv
# - חלון זמן: 14 ימים אחרונים
# - מניחים:
#   * עמלת Bybit: 0.10% לכל צד (per side)
#   * עמלת MEXC Futures: 0.01% לכל צד (Taker, Maker=0)
# ------------------------------------------------------------

import csv
import datetime as dt
from pathlib import Path

WINDOW_DAYS = 14

TRADES_PATH = Path(__file__).resolve().parent / "logs" / "trades.csv"


def parse_time(s: str):
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def pnl_of(row: dict) -> float:
    try:
        return float(row.get("pnl") or 0.0)
    except Exception:
        return 0.0


def notional_of(row: dict) -> float:
    """
    notional = price * qty
    משתמשים בכל שורה בקובץ (ENTER + יציאות),
    כי עמלה נגבית בכל פעולה.
    """
    try:
        price = float(row.get("price") or 0.0)
        qty = float(row.get("qty") or 0.0)
        return price * qty
    except Exception:
        return 0.0


def main():
    now = dt.datetime.now(dt.timezone.utc)
    since = now - dt.timedelta(days=WINDOW_DAYS)

    if not TRADES_PATH.exists():
        print("⚠️ trades.csv not found at", TRADES_PATH)
        return

    rows = []
    with TRADES_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            t = parse_time(r.get("time", ""))
            if not t or t < since:
                continue
            rows.append(r)

    print(f"📊 MEXC FEE SIMULATION – last {WINDOW_DAYS} days")
    print(f"Window: {since.isoformat()}  →  {now.isoformat()} (UTC)")
    print(f"Total log rows in window: {len(rows)}")
    print()

    if not rows:
        print("⚠️ No trades in this window.")
        return

    # נחשב רק על יציאות (TP1/TP2/SL/TIME) את ה-Gross PnL
    exit_trades = [
        r for r in rows
        if r.get("type") in ("TP1", "TP2", "SL", "TIME")
    ]

    gross_pnl = sum(pnl_of(r) for r in exit_trades)
    total_volume = sum(notional_of(r) for r in rows)

    # מודל עמלות – per side
    bybit_fee_rate = 0.001   # 0.10% לכל צד
    mexc_fee_rate  = 0.0001  # 0.01% לכל צד (Taker)

    fees_bybit = total_volume * bybit_fee_rate
    fees_mexc  = total_volume * mexc_fee_rate

    net_bybit = gross_pnl - fees_bybit
    net_mexc  = gross_pnl - fees_mexc

    print("====== GLOBAL SUMMARY (LAST 14 DAYS) ======")
    print(f"Exit trades counted:     {len(exit_trades)}")
    print(f"Gross PnL (exits only):  {gross_pnl:.4f} USDT")
    print(f"Total notional volume:   {total_volume:.2f} USDT")
    print()

    print("---- Bybit fee model ----")
    print(f"Assumed fee per side:    0.10%")
    print(f"Estimated total fees:    {fees_bybit:.4f} USDT")
    print(f"Net PnL after fees:      {net_bybit:.4f} USDT")
    print()

    print("---- MEXC futures fee model ----")
    print(f"Assumed fee per side:    0.01% (Taker, Maker≈0%)")
    print(f"Estimated total fees:    {fees_mexc:.4f} USDT")
    print(f"Net PnL after fees:      {net_mexc:.4f} USDT")
    print()

    # Break-even fee rate – באיזה אחוז עמלה פר צד אתה בנקודת האיזון
    if total_volume > 0:
        break_even_rate = gross_pnl / total_volume
        print("====== BREAK-EVEN FEE RATE ======")
        print(f"Break-even fee per side: {break_even_rate * 100:.4f}%")
        print("אם העמלה האמיתית נמוכה מזה – הבוט נטו ריווחי.")
    else:
        print("No volume in window – can't compute break-even fee rate.")


if __name__ == "__main__":
    main()
