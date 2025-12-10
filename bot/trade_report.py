# bot/trade_report.py
# ------------------------------------------------------------
# Summary report for the live bot:
# - Equity changes (full history + last N days)
# - Closed trades statistics from bot/logs/trades.csv
# - Open positions & unrealized PnL from Alpaca
#
# Usage on Render shell:
#   cd /opt/render/project/src
#   python bot/trade_report.py
# ------------------------------------------------------------

import os
from datetime import datetime, timedelta, timezone

import pandas as pd

try:
    import requests
except ImportError:
    requests = None


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

EXIT_EVENTS = {
    "TP1",
    "TP2",
    "SL",
    "TIME",
    "TIME_EOD",
    "TIME_SOFT",
    "SL_HARD",
    "DUST",
    "DUST-SL",
    "DUST-TIME",
}


def is_equity_symbol(symbol: str) -> bool:
    """
    Alpaca convention in הבוט:
    - מניות / ETF: סימבול בלי '/'
    - קריפטו: BTC/USD וכו'
    """
    if not isinstance(symbol, str):
        return False
    return "/" not in symbol


# ------------------------------------------------------------
# Equity report (from equity_curve.csv)
# ------------------------------------------------------------

def report_equity(days: int = 3):
    path = "bot/logs/equity_curve.csv"
    if not os.path.exists(path):
        print("=== EQUITY ===")
        print("No equity_curve.csv found.")
        print()
        return

    df = pd.read_csv(path)
    if "time" not in df.columns or "equity" not in df.columns:
        print("=== EQUITY ===")
        print("equity_curve.csv missing 'time' or 'equity' columns.")
        print()
        return

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["time", "equity"])

    if df.empty:
        print("=== EQUITY ===")
        print("No valid equity rows to analyze.")
        print()
        return

    df = df.sort_values("time")

    # Full history
    eq_start_all = df["equity"].iloc[0]
    eq_end_all = df["equity"].iloc[-1]
    change_all = eq_end_all - eq_start_all
    pct_all = (change_all / eq_start_all * 100.0) if eq_start_all != 0 else 0.0

    # Last N days
    last_ts = df["time"].max()
    cutoff = last_ts - timedelta(days=days)
    df_recent = df[df["time"] >= cutoff]
    eq_start_recent = df_recent["equity"].iloc[0]
    eq_end_recent = df_recent["equity"].iloc[-1]
    change_recent = eq_end_recent - eq_start_recent
    pct_recent = (change_recent / eq_start_recent * 100.0) if eq_start_recent != 0 else 0.0

    print("=== EQUITY (Alpaca-style, from equity_curve.csv) ===")
    print(f"History  : start={eq_start_all:.2f}  end={eq_end_all:.2f}  "
          f"change={change_all:.2f}  ({pct_all:.2f}%)")
    print(f"Last {days}d: start={eq_start_recent:.2f}  end={eq_end_recent:.2f}  "
          f"change={change_recent:.2f}  ({pct_recent:.2f}%)")
    print()

    # מחזיר בסוף לצורך שימוש אפשרי בהמשך
    return {
        "start_all": eq_start_all,
        "end_all": eq_end_all,
        "change_all": change_all,
        "pct_all": pct_all,
        "start_recent": eq_start_recent,
        "end_recent": eq_end_recent,
        "change_recent": change_recent,
        "pct_recent": pct_recent,
    }


# ------------------------------------------------------------
# Closed trades report (from trades.csv)
# ------------------------------------------------------------

def _closed_trades(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")
    df = df.dropna(subset=["time"])

    # רק הטריידים הסגורים
    df_exit = df[df["type"].isin(EXIT_EVENTS) & df["pnl"].notna()].copy()

    return df_exit


def summarize_trades(df_exit: pd.DataFrame, title: str = "TRADES") -> float:
    """
    מדפיס סיכום ל־df_exit ומחזיר את סכום ה-PnL.
    """
    print(f"=== {title} ===")

    if df_exit.empty:
        print("No closed trades to analyze (exit events with pnl).")
        print()
        return 0.0

    n_trades = len(df_exit)
    total_pnl = df_exit["pnl"].sum()

    wins = df_exit[df_exit["pnl"] > 0]
    losses = df_exit[df_exit["pnl"] < 0]

    n_wins = len(wins)
    n_losses = len(losses)
    winrate = (n_wins / n_trades * 100.0) if n_trades > 0 else 0.0

    avg_win = wins["pnl"].mean() if n_wins > 0 else 0.0
    avg_loss = losses["pnl"].mean() if n_losses > 0 else 0.0

    gross_profit = wins["pnl"].sum() if n_wins > 0 else 0.0
    gross_loss = losses["pnl"].sum() if n_losses > 0 else 0.0
    profit_factor = (
        gross_profit / abs(gross_loss)
        if gross_profit > 0 and gross_loss < 0
        else float("inf") if gross_profit > 0 and gross_loss == 0
        else 0.0
    )

    print(f"Closed trades: {n_trades}")
    print(f"Total PnL   : {total_pnl:.2f} $")
    print(f"Winrate     : {winrate:.1f} %")
    print(f"Avg win     : {avg_win:.2f} $")
    print(f"Avg loss    : {avg_loss:.2f} $")
    print(f"Gross profit: {gross_profit:.2f} $")
    print(f"Gross loss  : {gross_loss:.2f} $")
    print(f"Profit factor: {profit_factor:.2f}")
    print()

    # לפי צד
    for side_name in ("long", "short"):
        df_side = df_exit[df_exit["side"] == side_name]
        if df_side.empty:
            print(f"{side_name}: no trades")
            continue
        pnl_side = df_side["pnl"].sum()
        wr_side = (
            len(df_side[df_side["pnl"] > 0]) / len(df_side) * 100.0
            if len(df_side) > 0
            else 0.0
        )
        print(
            f"{side_name}: trades={len(df_side)}, PnL={pnl_side:.2f}, "
            f"winrate={wr_side:.1f}%"
        )
    print()

    # לפי “סוג נכס” – מניה/קריפטו (לפי הסימבול בלבד)
    df_exit["asset_type"] = df_exit["symbol"].apply(
        lambda s: "EQUITY" if is_equity_symbol(s) else "CRYPTO"
    )
    for t in ("EQUITY", "CRYPTO"):
        df_t = df_exit[df_exit["asset_type"] == t]
        if df_t.empty:
            print(f"{t}: no trades")
            continue
        pnl_t = df_t["pnl"].sum()
        wr_t = (
            len(df_t[df_t["pnl"] > 0]) / len(df_t) * 100.0
            if len(df_t) > 0
            else 0.0
        )
        print(
            f"{t}: trades={len(df_t)}, PnL={pnl_t:.2f}, "
            f"winrate={wr_t:.1f}%"
        )
    print()

    # סימבולים הכי טובים / גרועים
    by_symbol = df_exit.groupby("symbol")["pnl"].sum().sort_values(ascending=False)

    print("Top 10 symbols by PnL:")
    print(by_symbol.head(10))
    print()
    print("Worst 10 symbols by PnL:")
    print(by_symbol.tail(10))
    print()

    return float(total_pnl)


def report_trades(days: int = 3):
    path = "bot/logs/trades.csv"
    if not os.path.exists(path):
        print("=== TRADES ===")
        print("No trades.csv found.")
        print()
        return 0.0, 0.0

    df = pd.read_csv(path)
    if "time" not in df.columns or "pnl" not in df.columns:
        print("=== TRADES ===")
        print("trades.csv missing 'time' or 'pnl' columns.")
        print()
        return 0.0, 0.0

    df_exit_all = _closed_trades(df)
    total_pnl_all = summarize_trades(df_exit_all, "TRADES – ALL HISTORY")

    if df_exit_all.empty:
        return total_pnl_all, 0.0

    last_ts = df_exit_all["time"].max()
    cutoff = last_ts - timedelta(days=days)
    df_recent = df_exit_all[df_exit_all["time"] >= cutoff].copy()

    total_pnl_recent = summarize_trades(
        df_recent,
        f"TRADES – LAST {days} DAYS",
    )

    return total_pnl_all, total_pnl_recent


# ------------------------------------------------------------
# Open positions & unrealized PnL from Alpaca
# ------------------------------------------------------------

def report_alpaca_open_positions():
    if requests is None:
        print("=== Alpaca positions ===")
        print("requests package not available – cannot query Alpaca.")
        print()
        return 0.0

    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET_KEY")
    base_url = (
        os.getenv("APCA_API_BASE_URL")
        or os.getenv("ALPACA_BASE_URL")
        or "https://paper-api.alpaca.markets"
    )

    if not key or not secret:
        print("=== Alpaca positions ===")
        print("API keys not found in environment.")
        print()
        return 0.0

    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }

    # Account snapshot – equity, cash וכו'
    try:
        acc_resp = requests.get(
            base_url.rstrip("/") + "/v2/account",
            headers=headers,
            timeout=10,
        )
        acc_resp.raise_for_status()
        acc = acc_resp.json()
    except Exception as e:
        print("=== Alpaca positions ===")
        print(f"Failed to fetch /v2/account: {e}")
        print()
        return 0.0

    equity = float(acc.get("equity") or acc.get("portfolio_value") or 0.0)
    cash = float(acc.get("cash") or 0.0)
    long_mv = float(acc.get("long_market_value") or 0.0)
    short_mv = float(acc.get("short_market_value") or 0.0)
    buying_power = float(acc.get("buying_power") or 0.0)

    print("=== Alpaca /v2/account snapshot ===")
    print(f"equity          : {equity:.2f}")
    print(f"cash            : {cash:.2f}")
    print(f"long_market_val : {long_mv:.2f}")
    print(f"short_market_val: {short_mv:.2f}")
    print(f"buying_power    : {buying_power:.2f}")
    print()

    # Positions – unrealized PnL
    try:
        pos_resp = requests.get(
            base_url.rstrip("/") + "/v2/positions",
            headers=headers,
            timeout=10,
        )
        pos_resp.raise_for_status()
        positions = pos_resp.json()
    except Exception as e:
        print("=== Alpaca positions ===")
        print(f"Failed to fetch /v2/positions: {e}")
        print()
        return 0.0

    if not positions:
        print("=== Alpaca positions ===")
        print("No open positions.")
        print()
        return 0.0

    print("=== Alpaca – Open Positions (Unrealized PnL) ===")
    total_unreal = 0.0
    for p in positions:
        symbol = p.get("symbol", "")
        side = p.get("side", "")
        qty = float(p.get("qty") or 0.0)
        mval = float(p.get("market_value") or 0.0)
        upl = float(p.get("unrealized_pl") or 0.0)
        uplpc = float(p.get("unrealized_plpc") or 0.0)
        total_unreal += upl
        print(
            f"{symbol:10s} {side:5s} qty={qty:10.4f}  "
            f"MV={mval:10.2f}  UPL={upl:9.2f} ({uplpc:6.2f}%)"
        )

    print()
    print(f"Total unrealized PnL: {total_unreal:.2f} $")
    print()

    return total_unreal


# ------------------------------------------------------------
# main
# ------------------------------------------------------------

def main():
    days = 3  # אפשר לשנות אם תרצה

    print("\n========== BOT DAILY REPORT ==========\n")

    # Equity from CSV
    equity_info = report_equity(days=days)

    # Closed trades from CSV
    total_pnl_all, total_pnl_recent = report_trades(days=days)

    # Open positions (unrealized PnL) from Alpaca
    unreal = report_alpaca_open_positions()

    # Summary line
    print("========== SUMMARY ==========")
    print(f"Closed PnL (all history): {total_pnl_all:.2f} $")
    print(f"Closed PnL (last {days}d): {total_pnl_recent:.2f} $")
    print(f"Open PnL (unrealized)   : {unreal:.2f} $")
    if equity_info:
        print(f"Equity now (from CSV)   : {equity_info['end_all']:.2f} $")
    print("================================\n")


if __name__ == "__main__":
    main()
