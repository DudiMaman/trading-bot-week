# bot/analyze_with_fees.py
# ------------------------------------------------------------
# ניתוח ביצועים על כל trades.csv כולל הערכת עמלות
# ------------------------------------------------------------

from pathlib import Path
import csv
from collections import defaultdict

# שיעור עמלה משוער לכל צד (כניסה או יציאה)
# לפי החישוב מהקובץ שלך – 0.1% (0.001) מהשווי לכל צד
FEE_RATE = 0.001

TRADES = Path("bot/logs/trades.csv")


def pnl_of(row: dict) -> float:
    """מחזיר PnL מהשורה (או 0 אם אין/לא תקין)."""
    try:
        return float(row.get("pnl") or 0.0)
    except Exception:
        return 0.0


def main():
    if not TRADES.exists():
        print("⚠️ trades.csv not found at", TRADES)
        return

    rows: list[dict] = []
    with TRADES.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print("📊 FEE-AWARE PERFORMANCE (FULL HISTORY)")
    print(f"Total log rows in trades.csv: {len(rows)}")
    print()

    if not rows:
        print("⚠️ No trades found.")
        return

    enter_trades = [r for r in rows if r.get("type") == "ENTER"]
    exit_trades = [r for r in rows if r.get("type") in ("TP1", "TP2", "SL", "TIME")]

    # PnL גולמי (ללא עמלות) – רק מטריידי יציאה
    gross_pnl = sum(pnl_of(r) for r in exit_trades)

    # מחזור כולל (Notional volume) – *כל* השורות (כניסות + יציאות)
    total_volume = 0.0
    for row in rows:
        try:
            price = float(row.get("price") or 0.0)
            qty = float(row.get("qty") or 0.0)
        except Exception:
            continue
        notional = abs(price * qty)
        total_volume += notional

    est_fees = total_volume * FEE_RATE
    net_pnl = gross_pnl - est_fees

    print("====== GLOBAL SUMMARY (FULL HISTORY) ======")
    print(f"ENTER trades: {len(enter_trades)}")
    print(f"Exit trades:  {len(exit_trades)}")
    print(f"Gross PnL (exits only): {gross_pnl:.4f} USDT")
    print(f"Total notional volume:  {total_volume:.2f} USDT")
    print(f"Assumed fee rate per side: {FEE_RATE * 100:.3f}%")
    print(f"Estimated total fees:     {est_fees:.4f} USDT")
    print(f"Net PnL (gross - fees):   {net_pnl:.4f} USDT")
    print()

    # ---- סטטיסטיקה לפי צד (לונג / שורט) עם חלוקת עמלות לפי מחזור ----
    side_volume = defaultdict(float)
    side_pnl = defaultdict(float)

    # PnL לפי צד – רק ביציאות
    for row in exit_trades:
        side = row.get("side", "?")  # long / short
        side_pnl[side] += pnl_of(row)

    # מחזור לפי צד – מכל השורות (כניסות + יציאות)
    for row in rows:
        side = row.get("side", "?")
        try:
            price = float(row.get("price") or 0.0)
            qty = float(row.get("qty") or 0.0)
        except Exception:
            continue
        notional = abs(price * qty)
        side_volume[side] += notional

    total_vol_nonzero = sum(side_volume.values()) or 1.0

    print("====== BY SIDE (LONG / SHORT) – FULL HISTORY ======")
    print(f"{'side':6} {'volume':>12} {'gross_pnl':>12} {'est_fees':>12} {'net_pnl':>12}")
    for side in sorted(side_volume.keys()):
        vol = side_volume[side]
        share = vol / total_vol_nonzero
        fees_side = est_fees * share
        gross_side = side_pnl.get(side, 0.0)
        net_side = gross_side - fees_side
        print(f"{side:6} {vol:12.2f} {gross_side:12.4f} {fees_side:12.4f} {net_side:12.4f}")


if __name__ == "__main__":
    main()
