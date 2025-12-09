import pandas as pd
import numpy as np
from pathlib import Path


EXIT_TYPES = ["TP1", "TP2", "SL", "TIME", "DUST"]


def load_trades_csv(path_str: str = "bot/logs/trades.csv") -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        print(f"❌ file not found: {path}")
        return pd.DataFrame()

    df = pd.read_csv(path)

    # המרות בסיסיות
    for col in ("pnl", "qty", "price", "equity"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def filter_closed_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df[df["type"].isin(EXIT_TYPES)].copy()
    df = df[pd.notna(df["pnl"])]
    return df


def detect_asset_type(symbol: str) -> str:
    if isinstance(symbol, str) and "/" in symbol:
        return "CRYPTO"
    return "EQUITY"


def print_summary(df_exit: pd.DataFrame):
    if df_exit.empty:
        print("אין עדיין טריידים סגורים לניתוח.")
        return

    n_trades = len(df_exit)
    total_pnl = df_exit["pnl"].sum()

    wins = df_exit[df_exit["pnl"] > 0]
    losses = df_exit[df_exit["pnl"] < 0]

    winrate = (len(wins) / n_trades) * 100 if n_trades > 0 else 0.0
    avg_win = wins["pnl"].mean() if not wins.empty else 0.0
    avg_loss = losses["pnl"].mean() if not losses.empty else 0.0
    gross_profit = wins["pnl"].sum() if not wins.empty else 0.0
    gross_loss = losses["pnl"].sum() if not losses.empty else 0.0
    profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else np.nan

    print("===== SUMMARY =====")
    print(f"Total trades: {n_trades}")
    print(f"Total PnL: {total_pnl:.2f} $")
    print(f"Winrate: {winrate:.1f} %")
    print(f"Avg win: {avg_win:.2f} $")
    print(f"Avg loss: {avg_loss:.2f} $")
    print(f"Gross profit: {gross_profit:.2f} $")
    print(f"Gross loss: {gross_loss:.2f} $")
    print(f"Profit factor: {profit_factor:.2f}")
    print()


def print_by_side(df_exit: pd.DataFrame):
    print("=== By side (long/short) ===")
    for side in ["long", "short"]:
        df_side = df_exit[df_exit["side"] == side]
        if df_side.empty:
            print(f"{side}: no trades")
            continue
        pnl_side = df_side["pnl"].sum()
        wr_side = (df_side["pnl"] > 0).mean() * 100
        print(
            f"{side}: trades={len(df_side)}, "
            f"PnL={pnl_side:.2f}, winrate={wr_side:.1f}%"
        )
    print()


def print_by_asset_type(df_exit: pd.DataFrame):
    df_exit = df_exit.copy()
    df_exit["asset_type"] = df_exit["symbol"].apply(detect_asset_type)

    print("=== By asset type (EQUITY/CRYPTO) ===")
    for t in ["EQUITY", "CRYPTO"]:
        df_t = df_exit[df_exit["asset_type"] == t]
        if df_t.empty:
            print(f"{t}: no trades")
            continue
        pnl_t = df_t["pnl"].sum()
        wr_t = (df_t["pnl"] > 0).mean() * 100
        print(
            f"{t}: trades={len(df_t)}, "
            f"PnL={pnl_t:.2f}, winrate={wr_t:.1f}%"
        )
    print()


def print_symbols(df_exit: pd.DataFrame):
    print("=== Top 10 symbols by PnL ===")
    print(
        df_exit.groupby("symbol")["pnl"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    print()

    print("=== Worst 10 symbols by PnL ===")
    print(
        df_exit.groupby("symbol")["pnl"]
        .sum()
        .sort_values(ascending=True)
        .head(10)
    )
    print()


def main():
    df = load_trades_csv("bot/logs/trades.csv")
    if df.empty:
        return

    df_exit = filter_closed_trades(df)
    if df_exit.empty:
        print("אין עדיין טריידים סגורים לניתוח.")
        return

    print("===== RAW INFO =====")
    print(f"Total rows in CSV: {len(df)}")
    print(f"Unique symbols: {df['symbol'].nunique()}")
    print(f"Types: {df['type'].value_counts().to_dict()}")
    print()

    print_summary(df_exit)
    print_by_side(df_exit)
    print_by_asset_type(df_exit)
    print_symbols(df_exit)


if __name__ == "__main__":
    main()
