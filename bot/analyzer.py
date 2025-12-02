# bot/analyzer.py
# ------------------------------------------------------------
# Analysis engine v1:
# - קורא נתוני trades ו-equity מ-Postgres
# - מחשב ביצועים כלליים + לפי סימבול + לפי קונקטור
# - מחשב PnL יומי ו-Drawdown מהטבלת equity
# ------------------------------------------------------------

import os
from dataclasses import dataclass
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
import pandas as pd


@dataclass
class DBConfig:
    dsn: str


class Analyzer:
    def __init__(self, dsn: str):
        self.cfg = DBConfig(dsn=dsn)

    # ------------------------
    # Low-level fetchers
    # ------------------------
    def fetch_trades(self, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
        """
        מחזיר DataFrame של כל הטריידים מטבלת trades.
        עמודות: time, connector, symbol, type, side, price, qty, pnl, equity
        """
        with psycopg.connect(self.cfg.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    select time, connector, symbol, type, side, price, qty, pnl, equity
                    from trades
                    where time >= %s and time <= %s
                    order by time;
                    """,
                    (start, end),
                )
            elif start:
                cur.execute(
                    """
                    select time, connector, symbol, type, side, price, qty, pnl, equity
                    from trades
                    where time >= %s
                    order by time;
                    """,
                    (start,),
                )
            else:
                cur.execute(
                    """
                    select time, connector, symbol, type, side, price, qty, pnl, equity
                    from trades
                    order by time;
                    """
                )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(
                columns=["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"]
            )

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        # חלק מהשורות יכולות להיות בלי pnl (ENTER) – נמיר רק אם לא None
        if "pnl" in df.columns:
            df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")
        if "qty" in df.columns:
            df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")
        if "equity" in df.columns:
            df["equity"] = pd.to_numeric(df["equity"], errors="coerce")

        return df

    def fetch_equity(self, start: datetime | None = None, end: datetime | None = None) -> pd.DataFrame:
        """
        מחזיר DataFrame של equity לאורך זמן.
        עמודות: time, equity
        """
        with psycopg.connect(self.cfg.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    select time, equity
                    from equity
                    where time >= %s and time <= %s
                    order by time;
                    """,
                    (start, end),
                )
            elif start:
                cur.execute(
                    """
                    select time, equity
                    from equity
                    where time >= %s
                    order by time;
                    """,
                    (start,),
                )
            else:
                cur.execute(
                    """
                    select time, equity
                    from equity
                    order by time;
                    """
                )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(columns=["time", "equity"])

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
        return df

    # ------------------------
    # High-level summaries
    # ------------------------
    def summarize_overall(self, trades: pd.DataFrame) -> dict:
        """
        סיכום כללי של הביצועים מתוך trades.
        """
        if trades.empty:
            return {
                "num_trades": 0,
                "num_closed_legs": 0,
                "total_pnl": 0.0,
                "avg_pnl_per_leg": 0.0,
                "win_rate": 0.0,
                "loss_rate": 0.0,
            }

        # נחשב רק שורות שיש להן pnl (TP/SL/TIME, לא ENTER)
        closed = trades[trades["pnl"].notna()].copy()
        num_closed = len(closed)
        total_pnl = closed["pnl"].sum()

        wins = closed[closed["pnl"] > 0]
        losses = closed[closed["pnl"] < 0]

        win_rate = (len(wins) / num_closed) * 100 if num_closed > 0 else 0.0
        loss_rate = (len(losses) / num_closed) * 100 if num_closed > 0 else 0.0
        avg_pnl_per_leg = total_pnl / num_closed if num_closed > 0 else 0.0

        return {
            "num_trades": len(trades[trades["type"] == "ENTER"]),
            "num_closed_legs": num_closed,
            "total_pnl": float(total_pnl),
            "avg_pnl_per_leg": float(avg_pnl_per_leg),
            "win_rate": float(win_rate),
            "loss_rate": float(loss_rate),
        }

    def summarize_by_symbol(self, trades: pd.DataFrame) -> pd.DataFrame:
        """
        מחזיר טבלה עם ביצועים לפי סימבול:
        total_pnl, num_legs, win_rate, loss_rate
        """
        if trades.empty:
            return pd.DataFrame(
                columns=["symbol", "num_legs", "total_pnl", "avg_pnl_per_leg", "win_rate", "loss_rate"]
            )

        closed = trades[trades["pnl"].notna()].copy()
        if closed.empty:
            return pd.DataFrame(
                columns=["symbol", "num_legs", "total_pnl", "avg_pnl_per_leg", "win_rate", "loss_rate"]
            )

        def agg_symbol(group: pd.DataFrame):
            num_legs = len(group)
            total_pnl = group["pnl"].sum()
            wins = group[group["pnl"] > 0]
            losses = group[group["pnl"] < 0]
            win_rate = (len(wins) / num_legs) * 100 if num_legs > 0 else 0.0
            loss_rate = (len(losses) / num_legs) * 100 if num_legs > 0 else 0.0
            avg_pnl = total_pnl / num_legs if num_legs > 0 else 0.0
            return pd.Series(
                {
                    "num_legs": num_legs,
                    "total_pnl": total_pnl,
                    "avg_pnl_per_leg": avg_pnl,
                    "win_rate": win_rate,
                    "loss_rate": loss_rate,
                }
            )

        by_sym = closed.groupby("symbol", as_index=False).apply(agg_symbol)
        # נסדר לפי תרומה ל-PnL מהגבוה לנמוך
        by_sym = by_sym.sort_values("total_pnl", ascending=False).reset_index(drop=True)
        return by_sym

    def summarize_by_connector(self, trades: pd.DataFrame) -> pd.DataFrame:
        """
        ביצועים לפי קונקטור (bybit / alpaca / וכו').
        """
        if trades.empty:
            return pd.DataFrame(
                columns=["connector", "num_legs", "total_pnl", "avg_pnl_per_leg", "win_rate", "loss_rate"]
            )

        closed = trades[trades["pnl"].notna()].copy()
        if closed.empty:
            return pd.DataFrame(
                columns=["connector", "num_legs", "total_pnl", "avg_pnl_per_leg", "win_rate", "loss_rate"]
            )

        def agg_conn(group: pd.DataFrame):
            num_legs = len(group)
            total_pnl = group["pnl"].sum()
            wins = group[group["pnl"] > 0]
            losses = group[group["pnl"] < 0]
            win_rate = (len(wins) / num_legs) * 100 if num_legs > 0 else 0.0
            loss_rate = (len(losses) / num_legs) * 100 if num_legs > 0 else 0.0
            avg_pnl = total_pnl / num_legs if num_legs > 0 else 0.0
            return pd.Series(
                {
                    "num_legs": num_legs,
                    "total_pnl": total_pnl,
                    "avg_pnl_per_leg": avg_pnl,
                    "win_rate": win_rate,
                    "loss_rate": loss_rate,
                }
            )

        by_conn = closed.groupby("connector", as_index=False).apply(agg_conn)
        by_conn = by_conn.sort_values("total_pnl", ascending=False).reset_index(drop=True)
        return by_conn

    def summarize_equity_curve(self, eq: pd.DataFrame) -> dict:
        """
        מחשב נתונים עיקריים מה-equity:
        - equity_start, equity_end, total_return_pct
        - max_drawdown_pct
        """
        if eq.empty:
            return {
                "equity_start": 0.0,
                "equity_end": 0.0,
                "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0,
            }

        eq = eq.sort_values("time").copy()
        eq["equity"] = eq["equity"].astype(float)

        equity_start = float(eq["equity"].iloc[0])
        equity_end = float(eq["equity"].iloc[-1])
        total_return_pct = (
            ((equity_end / equity_start) - 1.0) * 100 if equity_start > 0 else 0.0
        )

        # מקסימום drawdown
        roll_max = eq["equity"].cummax()
        drawdown = (eq["equity"] - roll_max) / roll_max
        max_drawdown_pct = float(drawdown.min() * 100.0) if len(drawdown) > 0 else 0.0

        return {
            "equity_start": equity_start,
            "equity_end": equity_end,
            "total_return_pct": total_return_pct,
            "max_drawdown_pct": max_drawdown_pct,
        }

    # ------------------------
    # Convenience: run full analysis
    # ------------------------
    def run_full(self, start: datetime | None = None, end: datetime | None = None):
        trades = self.fetch_trades(start, end)
        equity = self.fetch_equity(start, end)

        overall = self.summarize_overall(trades)
        by_symbol = self.summarize_by_symbol(trades)
        by_connector = self.summarize_by_connector(trades)
        eq_stats = self.summarize_equity_curve(equity)

        return {
            "overall": overall,
            "by_symbol": by_symbol,
            "by_connector": by_connector,
            "equity_stats": eq_stats,
        }


def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌ DATABASE_URL is not set – cannot run analyzer.")
        return

    analyzer = Analyzer(dsn=dsn)
    results = analyzer.run_full()

    print("\n====== OVERALL PERFORMANCE ======")
    o = results["overall"]
    print(f"Num trades (ENTER):     {o['num_trades']}")
    print(f"Num closed legs:        {o['num_closed_legs']}")
    print(f"Total PnL:              {o['total_pnl']:.2f}")
    print(f"Avg PnL per leg:        {o['avg_pnl_per_leg']:.2f}")
    print(f"Win rate:               {o['win_rate']:.1f}%")
    print(f"Loss rate:              {o['loss_rate']:.1f}%")

    print("\n====== BY CONNECTOR ======")
    print(results["by_connector"].to_string(index=False))

    print("\n====== BY SYMBOL (TOP/BOTTOM) ======")
    by_sym = results["by_symbol"]
    # אם יש הרבה, נראה רק את הטופ והבוטם
    if len(by_sym) > 20:
        top = by_sym.head(10)
        bottom = by_sym.tail(10)
        print("\n-- TOP 10 SYMBOLS --")
        print(top.to_string(index=False))
        print("\n-- BOTTOM 10 SYMBOLS --")
        print(bottom.to_string(index=False))
    else:
        print(by_sym.to_string(index=False))

    print("\n====== EQUITY STATS ======")
    es = results["equity_stats"]
    print(f"Equity start:           {es['equity_start']:.2f}")
    print(f"Equity end:             {es['equity_end']:.2f}")
    print(f"Total return:           {es['total_return_pct']:.2f}%")
    print(f"Max drawdown:           {es['max_drawdown_pct']:.2f}%")


if __name__ == "__main__":
    main()
