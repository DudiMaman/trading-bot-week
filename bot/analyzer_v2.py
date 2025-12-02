# bot/analyzer_v2.py
# ------------------------------------------------------------
# Analysis engine v2 על live_trades:
# - מחשב R לכל טרייד (realized_pnl / risk_usd)
# - מזהה טריידים "קטסטרופליים" (הפסד גדול ב-R או באחוז מההון)
# - נותן סיכום לפי סימבול + סיכום כללי
# ------------------------------------------------------------

import os
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row
import pandas as pd


@dataclass
class DBConfig:
    dsn: str


class LiveTradesAnalyzer:
    def __init__(self, dsn: str):
        self.cfg = DBConfig(dsn=dsn)

    # ------------------------
    # Fetch
    # ------------------------
    def fetch_live_trades(self) -> pd.DataFrame:
        """
        מביא את live_trades מה-DB.
        מתמקד בטריידים שסגורים (closed_at לא NULL).
        """
        with psycopg.connect(self.cfg.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                select
                    id,
                    connector,
                    symbol,
                    side,
                    entry_price,
                    qty,
                    risk_usd,
                    equity_at_entry,
                    config_id,
                    opened_at,
                    exit_price,
                    realized_pnl,
                    exit_type,
                    equity_at_exit,
                    closed_at
                from live_trades
                where closed_at is not null
                order by opened_at;
                """
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(
                columns=[
                    "id",
                    "connector",
                    "symbol",
                    "side",
                    "entry_price",
                    "qty",
                    "risk_usd",
                    "equity_at_entry",
                    "config_id",
                    "opened_at",
                    "exit_price",
                    "realized_pnl",
                    "exit_type",
                    "equity_at_exit",
                    "closed_at",
                ]
            )

        df = pd.DataFrame(rows)
        # המרות טיפוסים בסיסיות
        time_cols = ["opened_at", "closed_at"]
        for c in time_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")

        num_cols = [
            "entry_price",
            "qty",
            "risk_usd",
            "equity_at_entry",
            "exit_price",
            "realized_pnl",
            "equity_at_exit",
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # חישובי R ו-% מההון
        df["R"] = df.apply(
            lambda r: (r["realized_pnl"] / r["risk_usd"])
            if (r.get("risk_usd") and r["risk_usd"] != 0)
            else None,
            axis=1,
        )
        df["pnl_pct_equity"] = df.apply(
            lambda r: (r["realized_pnl"] / r["equity_at_entry"] * 100.0)
            if (r.get("equity_at_entry") and r["equity_at_entry"] != 0)
            else None,
            axis=1,
        )

        return df

    # ------------------------
    # Summaries
    # ------------------------
    def summarize_overall(self, df: pd.DataFrame) -> dict:
        if df.empty:
            return {
                "num_trades": 0,
                "total_pnl": 0.0,
                "avg_R": 0.0,
                "median_R": 0.0,
                "win_rate": 0.0,
                "loss_rate": 0.0,
            }

        closed = df[df["realized_pnl"].notna()].copy()
        if closed.empty:
            return {
                "num_trades": 0,
                "total_pnl": 0.0,
                "avg_R": 0.0,
                "median_R": 0.0,
                "win_rate": 0.0,
                "loss_rate": 0.0,
            }

        num_trades = len(closed)
        total_pnl = float(closed["realized_pnl"].sum())

        wins = closed[closed["realized_pnl"] > 0]
        losses = closed[closed["realized_pnl"] < 0]

        win_rate = (len(wins) / num_trades) * 100 if num_trades > 0 else 0.0
        loss_rate = (len(losses) / num_trades) * 100 if num_trades > 0 else 0.0

        avg_R = float(closed["R"].mean()) if "R" in closed.columns else 0.0
        median_R = float(closed["R"].median()) if "R" in closed.columns else 0.0

        return {
            "num_trades": num_trades,
            "total_pnl": total_pnl,
            "avg_R": avg_R,
            "median_R": median_R,
            "win_rate": win_rate,
            "loss_rate": loss_rate,
        }

    def summarize_by_symbol(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "num_trades",
                    "total_pnl",
                    "avg_R",
                    "median_R",
                    "win_rate",
                    "loss_rate",
                ]
            )

        closed = df[df["realized_pnl"].notna()].copy()
        if closed.empty:
            return pd.DataFrame(
                columns=[
                    "symbol",
                    "num_trades",
                    "total_pnl",
                    "avg_R",
                    "median_R",
                    "win_rate",
                    "loss_rate",
                ]
            )

        def agg_sym(group: pd.DataFrame):
            num_trades = len(group)
            total_pnl = float(group["realized_pnl"].sum())
            avg_R = float(group["R"].mean())
            median_R = float(group["R"].median())
            wins = group[group["realized_pnl"] > 0]
            losses = group[group["realized_pnl"] < 0]
            win_rate = (len(wins) / num_trades) * 100 if num_trades > 0 else 0.0
            loss_rate = (len(losses) / num_trades) * 100 if num_trades > 0 else 0.0
            return pd.Series(
                {
                    "num_trades": num_trades,
                    "total_pnl": total_pnl,
                    "avg_R": avg_R,
                    "median_R": median_R,
                    "win_rate": win_rate,
                    "loss_rate": loss_rate,
                }
            )

        by_sym = closed.groupby("symbol", as_index=False).apply(agg_sym)
        by_sym = by_sym.sort_values("total_pnl", ascending=False).reset_index(drop=True)
        return by_sym

    def worst_trades(self, df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """
        מחזיר את n הטריידים הכי גרועים לפי realized_pnl.
        """
        closed = df[df["realized_pnl"].notna()].copy()
        if closed.empty:
            return closed

        worst = closed.sort_values("realized_pnl", ascending=True).head(n)
        return worst

    def catastrophic_trades(
        self,
        df: pd.DataFrame,
        min_R: float = -2.0,
        min_loss_pct_equity: float = -2.0,
    ) -> pd.DataFrame:
        """
        מחפש טריידים "קטסטרופלים":
        - R קטן מ-min_R (למשל -2R)
        - או הפסד באחוז מההון קטן מ-min_loss_pct_equity (למשל -2%)
        """
        closed = df[df["realized_pnl"].notna()].copy()
        if closed.empty:
            return closed

        cond_R = closed["R"].notna() & (closed["R"] <= min_R)
        cond_pct = closed["pnl_pct_equity"].notna() & (
            closed["pnl_pct_equity"] <= min_loss_pct_equity
        )

        cats = closed[cond_R | cond_pct].copy()
        cats = cats.sort_values("realized_pnl", ascending=True)
        return cats

    # ------------------------
    # Run full analysis
    # ------------------------
    def run_full(self):
        df = self.fetch_live_trades()
        overall = self.summarize_overall(df)
        by_symbol = self.summarize_by_symbol(df)
        worst = self.worst_trades(df, n=10)
        cats = self.catastrophic_trades(df, min_R=-2.0, min_loss_pct_equity=-2.0)

        return {
            "df": df,
            "overall": overall,
            "by_symbol": by_symbol,
            "worst_trades": worst,
            "catastrophic_trades": cats,
        }


def main():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("❌ DATABASE_URL is not set – cannot run analyzer_v2.")
        return

    analyzer = LiveTradesAnalyzer(dsn=dsn)
    res = analyzer.run_full()

    print("\n====== OVERALL (LIVE TRADES) ======")
    o = res["overall"]
    print(f"Num trades:      {o['num_trades']}")
    print(f"Total PnL:       {o['total_pnl']:.2f}")
    print(f"Avg R:           {o['avg_R']:.2f}")
    print(f"Median R:        {o['median_R']:.2f}")
    print(f"Win rate:        {o['win_rate']:.1f}%")
    print(f"Loss rate:       {o['loss_rate']:.1f}%")

    print("\n====== BY SYMBOL (LIVE TRADES) ======")
    if not res["by_symbol"].empty:
        print(res["by_symbol"].to_string(index=False))
    else:
        print("(no closed trades yet)")

    print("\n====== WORST TRADES (BY PnL) ======")
    worst = res["worst_trades"]
    if not worst.empty:
        cols = ["id", "connector", "symbol", "side", "realized_pnl", "R", "pnl_pct_equity", "exit_type"]
        print(worst[cols].to_string(index=False))
    else:
        print("(no closed trades)")

    print("\n====== CATASTROPHIC TRADES (R <= -2 OR loss <= -2% equity) ======")
    cats = res["catastrophic_trades"]
    if not cats.empty:
        cols = ["id", "connector", "symbol", "side", "realized_pnl", "R", "pnl_pct_equity", "exit_type"]
        print(cats[cols].to_string(index=False))
    else:
        print("(no catastrophic trades by current thresholds)")


if __name__ == "__main__":
    main()
