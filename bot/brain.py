# bot/brain.py
# ------------------------------------------------------------
# "מוח" אוטומטי לבוט:
# - קורא את live_trades + equity
# - מזהה טריידים קטסטרופליים וסימבולים רעילים
# - מתאים risk_per_trade
# - חוסם סימבולים בעייתיים
# התוצר נכתב לטבלאות:
#   - bot_settings
#   - symbol_overrides
# והבוט (run_live_week) יקרא אותן תוך כדי ריצה.
# ------------------------------------------------------------

import os
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row
import pandas as pd

from bot.db_writer import DB
from bot.analyzer_v2 import LiveTradesAnalyzer


@dataclass
class BrainConfig:
    dsn: str
    catastrophic_R: float = -2.0           # טרייד עם R <= -2 נחשב קטסטרופה
    catastrophic_loss_pct: float = -2.0    # או הפסד >= 2% מההון בטרייד אחד
    dd_low_risk: float = 5.0               # מתחת ל-5% DD אפשר סיכון "רגיל"
    dd_med_risk: float = 10.0              # עד 10% DD – סיכון בינוני
    risk_low: float = 0.003                # 0.3% מההון בטרייד
    risk_med: float = 0.005                # 0.5%
    risk_high: float = 0.01                # 1%


class BotBrain:
    def __init__(self, dsn: str):
        self.cfg = BrainConfig(dsn=dsn)
        self.db = DB(dsn)
        self.analyzer = LiveTradesAnalyzer(dsn)

    # ------------------------
    # Equity & Drawdown
    # ------------------------
    def fetch_equity_series(self) -> pd.DataFrame:
        """
        מביא את equity לאורך זמן ומחשב max drawdown.
        """
        with psycopg.connect(self.cfg.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            cur.execute(
                "select time, equity from equity order by time;"
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame(columns=["time", "equity"])

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
        df = df.dropna(subset=["time", "equity"]).sort_values("time")
        return df

    def compute_max_drawdown(self, df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0

        equity = df["equity"].values
        peak = equity[0]
        max_dd = 0.0
        for eq in equity:
            if eq > peak:
                peak = eq
            dd = (eq - peak) / peak * 100.0
            if dd < max_dd:
                max_dd = dd
        return abs(max_dd)

    # ------------------------
    # חוקים
    # ------------------------
    def decide_risk_per_trade(self, max_dd: float) -> float:
        """
        מחליט על risk_per_trade לפי drawdown.
        """
        cfg = self.cfg
        if max_dd <= cfg.dd_low_risk:
            return cfg.risk_high
        elif max_dd <= cfg.dd_med_risk:
            return cfg.risk_med
        else:
            return cfg.risk_low

    def run(self):
        # 1. ניתוח live_trades באמצעות analyzer_v2
        res = self.analyzer.run_full()
        df_trades = res["df"]
        cats = res["catastrophic_trades"]

        # 2. חישוב max drawdown לפי equity
        eq = self.fetch_equity_series()
        max_dd = self.compute_max_drawdown(eq)

        print("====== BRAIN: INPUT STATE ======")
        o = res["overall"]
        print(f"Num trades:          {o['num_trades']}")
        print(f"Total PnL:           {o['total_pnl']:.2f}")
        print(f"Avg R:               {o['avg_R']:.2f}")
        print(f"Win rate:            {o['win_rate']:.1f}%")
        print(f"Max drawdown:        {max_dd:.2f}%")
        print(f"Catastrophic trades: {len(cats)}")

        # 3. החלטה על risk_per_trade
        base_risk = self.decide_risk_per_trade(max_dd)

        # אם יש טריידים קטסטרופליים אחרונים – נקשיח עוד קצת
        if not cats.empty:
            # למשל אם ב-24 שעות האחרונות היה טרייד קטסטרופלי – נחתוך עוד
            latest_closed = cats["closed_at"].max()
            # (לא נכנסים לדקויות timezone – מספיק לנו כעיקרון)
            print(
                f"[BRAIN] Found catastrophic trades, last at {latest_closed}. "
                f"Reducing risk a bit."
            )
            base_risk = min(base_risk, self.cfg.risk_med)

        print(f"[BRAIN] decided risk_per_trade = {base_risk:.4f}")

        # 4. עדכון bot_settings (שימושי ל-run_live_week)
        self.db.set_bot_setting("risk_per_trade", f"{base_risk:.6f}")

        # 5. זיהוי סימבולים "רעילים" – לבלאקליסט
        #    כל סימבול שמופיע בקטסטרופליים – נסמן אותו כ-block_new_trades
        bad_symbols = []
        if not cats.empty:
            bad_symbols = sorted(cats["symbol"].dropna().unique())
            for sym in bad_symbols:
                reason = "catastrophic trades (R<=-2 or loss_pct<=-2)"
                self.db.upsert_symbol_override(
                    symbol=sym,
                    block_new_trades=True,
                    last_reason=reason,
                )

        print("\n[BRAIN] Symbol overrides (blocked):")
        if bad_symbols:
            for s in bad_symbols:
                print(f"  - {s} (blocked_new_trades=True)")
        else:
            print("  none (no new catastrophic symbols)")

        print("\n[BRAIN] Done. Settings written to DB.")
