# bot/analyzer_v2.py
# ------------------------------------------------------------
# "מוח" אנליטי לבוט:
# - קורא trades סגורים מטבלת live_trades
# - מחשב ביצועים אחרונים (50 / 200 טריידים)
# - מחליט מצב עבודה: DEFENSIVE / NORMAL / AGGRESSIVE
# - קובע risk_per_trade, max_portfolio_exposure וכו'
# - מזהה סימבולים בעייתיים ומכניס להקפאה זמנית
# - שומר snapshot ל-bot_settings (לא חובה לבוט כדי לעבוד)
# ------------------------------------------------------------

from __future__ import annotations

import os
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Dict, List, Set

import psycopg
from psycopg.rows import dict_row


# ------------------------------------------------------------
# Dataclass שמוחזר ל-run_live_week
# ------------------------------------------------------------
@dataclass
class BrainSettings:
    mode: str                      # "DEFENSIVE" / "NORMAL" / "AGGRESSIVE"
    risk_per_trade: float          # אחוז מההון ב-Risk לכל טרייד
    max_portfolio_exposure: float  # חלק מההון שמוקצה לפוזיציות פתוחות (מצטבר)
    max_notional_pct_hard: float   # Hard cap לפוזיציה בודדת מן התיק
    atr_k_sl: float                # כמה ATR ל-SL מהכניסה
    r1_R: float                    # TP1 ברווח של כמה R
    r2_R: float                    # TP2 ברווח של כמה R
    p1_pct: float                  # איזה חלק מהפוזיציה נסגר ב-TP1
    p2_pct: float                  # איזה חלק נסגר ב-TP2
    be_after_R: float              # אחרי כמה R מעבירים SL ל-BE
    trail_atr_k: float             # טריילינג SL לפי ATR
    max_bars_in_trade: int         # מקסימום ברים לפוזיציה
    blocked_symbols: Set[str]      # סימבולים חסומים כרגע


# ------------------------------------------------------------
# קונפיג ברירת מחדל
# ------------------------------------------------------------

MAX_NOTIONAL_PCT_HARD_DEFAULT = 0.20  # לא יותר מ-20% מהתיק לפוזיציה אחת

MODE_CONFIGS = {
    "DEFENSIVE": {
        "risk_per_trade": 0.003,        # 0.3%
        "max_portfolio_exposure": 0.30, # עד 30% הון חשוף
        "trail_atr_k": 1.0,             # טריילינג צמוד יותר
    },
    "NORMAL": {
        "risk_per_trade": 0.005,        # 0.5%
        "max_portfolio_exposure": 0.60, # עד 60% הון חשוף
        "trail_atr_k": 1.2,
    },
    "AGGRESSIVE": {
        "risk_per_trade": 0.008,        # 0.8%
        "max_portfolio_exposure": 0.80, # עד 80% הון חשוף
        "trail_atr_k": 1.0,             # אגרסיבי יותר
    },
}

# פרמטרים קבועים (אפשר לשנות בעתיד בקלות)
ATR_K_SL = 1.5
R1_R = 1.0
R2_R = 2.5
P1_PCT = 0.5
P2_PCT = 0.5
BE_AFTER_R = 0.8
MAX_BARS_IN_TRADE = 48

# ------------------------------------------------------------
# עזר לחישובי ביצועים
# ------------------------------------------------------------

def _calc_R(trade: dict) -> float | None:
    """מחזיר R = pnl / risk_usd, אם אפשר לחשב; אחרת None."""
    try:
        risk = float(trade.get("risk_usd") or 0.0)
        pnl = float(trade.get("realized_pnl") or 0.0)
        if risk <= 0:
            return None
        return pnl / risk
    except Exception:
        return None


def _compute_win_and_avgR(trades: Iterable[dict]) -> tuple[float | None, float | None]:
    """מחזיר (win_rate, avg_R) או (None, None) אם אין מספיק נתונים."""
    trades = list(trades)
    if not trades:
        return None, None

    wins = 0
    Rs: List[float] = []
    for t in trades:
        pnl = t.get("realized_pnl")
        if pnl is None:
            continue
        if pnl > 0:
            wins += 1
        R = _calc_R(t)
        if R is not None:
            Rs.append(R)

    if not Rs:
        return None, None

    win_rate = wins / len(trades)
    avg_R = sum(Rs) / len(Rs)
    return win_rate, avg_R


def _compute_equity_change_pct(trades: List[dict]) -> float | None:
    """
    הערכה גסה לשינוי ההון באחוזים על פני החלון:
    לוקחים equity_at_entry בתחילת החלון ו-equity_at_exit בסופו.
    """
    if not trades:
        return None

    # מסדרים לפי זמן סגירה מהישן לחדש
    ordered = sorted(
        [t for t in trades if t.get("closed_at")],
        key=lambda t: t["closed_at"],
    )
    if len(ordered) < 2:
        return None

    first = ordered[0]
    last = ordered[-1]

    eq_start = float(first.get("equity_at_entry") or first.get("equity_at_exit") or 0.0)
    eq_end = float(last.get("equity_at_exit") or 0.0)

    if eq_start <= 0 or eq_end <= 0:
        return None

    return (eq_end - eq_start) / eq_start


# ------------------------------------------------------------
# קריאה מה-DB
# ------------------------------------------------------------

def _fetch_closed_trades(dsn: str, config_id: str, limit: int = 200) -> List[dict]:
    """
    מחזיר עד 'limit' טריידים סגורים מטבלת live_trades עבור config_id.
    """
    with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as conn:
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
            where config_id = %s
              and realized_pnl is not null
              and closed_at is not null
            order by closed_at desc
            limit %s;
            """,
            (config_id, limit),
        )
        rows = cur.fetchall()
        return rows


def _fetch_blocked_symbols(dsn: str, config_id: str, now_utc: datetime) -> Set[str]:
    """
    קורא מטבלת symbol_overrides כל סימבול שחסום כרגע (block_until עתידי או NULL).
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                select symbol, block_until
                from symbol_overrides
                where config_id = %s
                  and is_blocked = true;
                """,
                (config_id,),
            )
            symbols: Set[str] = set()
            for row in cur.fetchall():
                block_until = row.get("block_until")
                if block_until is None or block_until > now_utc:
                    symbols.add(row["symbol"])
            return symbols
    except Exception as e:
        print(f"[WARN] _fetch_blocked_symbols failed: {e}")
        return set()


def _insert_brain_snapshot(dsn: str, config_id: str, payload: dict):
    """
    שומר snapshot קטן ל-bot_settings לצורך דיבוג/תיעוד.
    לא קריטי – עטוף ב-try/except.
    """
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                insert into bot_settings (config_id, key, value)
                values (%s, %s, %s::jsonb);
                """,
                (config_id, "brain_state", json.dumps(payload)),
            )
    except Exception as e:
        print(f"[WARN] _insert_brain_snapshot failed: {e}")


def _mark_symbol_blocked(dsn: str, config_id: str, symbol: str, now_utc: datetime):
    """
    מסמן סימבול כ"חסום ליומיים" בטבלת symbol_overrides.
    לא מוחק רשומות ישנות – פשוט מוסיף.
    """
    note = "auto-blocked: persistent underperformance vs portfolio baseline"
    block_until = now_utc + timedelta(days=2)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                insert into symbol_overrides
                    (config_id, symbol, is_blocked, block_until, note)
                values (%s, %s, true, %s, %s);
                """,
                (config_id, symbol, block_until, note),
            )
    except Exception as e:
        print(f"[WARN] _mark_symbol_blocked failed for {symbol}: {e}")


# ------------------------------------------------------------
# לוגיקת המוח: קובע מצב + פרמטרים + חסימות
# ------------------------------------------------------------

def get_brain_settings(config_id: str = "SAFE_V1") -> BrainSettings:
    """
    פונקציה אחת שהבוט קורא:
    - מושכת טריידים סגורים
    - מחשבת ביצועים אחרונים
    - קובעת מצב (mode) ופרמטרים
    - מזהה סימבולים בעייתיים ומכניסה להקפאה
    - מחזירה BrainSettings לבוט
    """
    dsn = os.getenv("DATABASE_URL")
    now_utc = datetime.now(timezone.utc)

    # אם אין DB – חוזרים לברירות מחדל "NORMAL"
    if not dsn:
        return BrainSettings(
            mode="NORMAL",
            risk_per_trade=MODE_CONFIGS["NORMAL"]["risk_per_trade"],
            max_portfolio_exposure=MODE_CONFIGS["NORMAL"]["max_portfolio_exposure"],
            max_notional_pct_hard=MAX_NOTIONAL_PCT_HARD_DEFAULT,
            atr_k_sl=ATR_K_SL,
            r1_R=R1_R,
            r2_R=R2_R,
            p1_pct=P1_PCT,
            p2_pct=P2_PCT,
            be_after_R=BE_AFTER_R,
            trail_atr_k=MODE_CONFIGS["NORMAL"]["trail_atr_k"],
            max_bars_in_trade=MAX_BARS_IN_TRADE,
            blocked_symbols=set(),
        )

    # מושכים עד 200 טריידים סגורים אחרונים
    all_trades = _fetch_closed_trades(dsn, config_id, limit=200)
    if len(all_trades) < 10:
        # עדיין אין מספיק היסטוריה – עוברים ל-NORMAL עם הערכים הבסיסיים
        blocked = _fetch_blocked_symbols(dsn, config_id, now_utc)
        return BrainSettings(
            mode="NORMAL",
            risk_per_trade=MODE_CONFIGS["NORMAL"]["risk_per_trade"],
            max_portfolio_exposure=MODE_CONFIGS["NORMAL"]["max_portfolio_exposure"],
            max_notional_pct_hard=MAX_NOTIONAL_PCT_HARD_DEFAULT,
            atr_k_sl=ATR_K_SL,
            r1_R=R1_R,
            r2_R=R2_R,
            p1_pct=P1_PCT,
            p2_pct=P2_PCT,
            be_after_R=BE_AFTER_R,
            trail_atr_k=MODE_CONFIGS["NORMAL"]["trail_atr_k"],
            max_bars_in_trade=MAX_BARS_IN_TRADE,
            blocked_symbols=blocked,
        )

    # חלון אחרון (50 טריידים) – משתמשים בפחות אם אין
    recent_window = all_trades[:50] if len(all_trades) >= 50 else all_trades
    recent_trades = list(recent_window)
    # גלובלי – כל מה שהבאנו (עד 200)
    global_trades = list(all_trades)

    # מחשבי ביצועים
    win50, avgR50 = _compute_win_and_avgR(recent_trades)
    win_all, avgR_all = _compute_win_and_avgR(global_trades)
    pnl50_pct = _compute_equity_change_pct(recent_trades)

    # הגנות נגד None
    win50 = win50 if win50 is not None else 0.0
    avgR50 = avgR50 if avgR50 is not None else 0.0
    win_all = win_all if win_all is not None else win50
    avgR_all = avgR_all if avgR_all is not None else avgR50
    pnl50_pct = pnl50_pct if pnl50_pct is not None else 0.0

    # --------------------------------------------------------
    # קביעת מצב עבודה (mode)
    # --------------------------------------------------------
    # 1) מצב דפנסיבי – הפסדים מתמשכים או R שלילי
    if pnl50_pct <= -0.10 or avgR50 <= -0.25:
        mode = "DEFENSIVE"
    # 2) מצב התקפי – רווחים חזקים ו-R טוב
    elif pnl50_pct >= 0.10 and avgR50 >= 0.7:
        mode = "AGGRESSIVE"
    # אחרת – בסיסי
    else:
        mode = "NORMAL"

    cfg = MODE_CONFIGS.get(mode, MODE_CONFIGS["NORMAL"])

    # --------------------------------------------------------
    # איתור סימבולים בעייתיים (יחסית לפורטפוליו)
    # --------------------------------------------------------
    # בסיס להשוואה – היסטוריה גלובלית
    symbol_trades: Dict[str, List[dict]] = {}
    for t in global_trades:
        sym = t.get("symbol")
        if not sym:
            continue
        symbol_trades.setdefault(sym, []).append(t)

    bad_symbols: Set[str] = set()
    for sym, tlist in symbol_trades.items():
        if len(tlist) < 8:
            continue  # מעט מדי טריידים לסימבול הזה

        w_sym, R_sym = _compute_win_and_avgR(tlist)
        if w_sym is None or R_sym is None:
            continue

        # בודקים חריגה לעומת הנורמה – לא מספר קשיח 30%
        # תנאי:
        # 1. win_rate נמוך לפחות ב-20 נק' אחוז מהפורטפוליו
        # 2. avg_R נמוך לפחות ב-0.15 מהפורטפוליו
        # 3. avg_R שלילי בעצמו
        if (
            w_sym <= max(0.0, win_all - 0.20)
            and R_sym <= avgR_all - 0.15
            and R_sym < 0
        ):
            bad_symbols.add(sym)

    # מסמן סימבולים בעייתיים כחסומים ליומיים (לא מחכה "שתי סריקות רצופות",
    # אלא דורש 8+ טריידים וחריגה משמעותית – זו ההקשחה בפועל).
    for sym in bad_symbols:
        _mark_symbol_blocked(dsn, config_id, sym, now_utc)

    # מושך שוב את רשימת החסומים בפועל
    blocked_symbols = _fetch_blocked_symbols(dsn, config_id, now_utc)

    # --------------------------------------------------------
    # לוג מצבו של המוח ל-bot_settings
    # --------------------------------------------------------
    snapshot = {
        "ts": now_utc.isoformat(),
        "config_id": config_id,
        "mode": mode,
        "win50": win50,
        "avgR50": avgR50,
        "pnl50_pct": pnl50_pct,
        "win_all": win_all,
        "avgR_all": avgR_all,
        "blocked_symbols": sorted(list(blocked_symbols)),
        "risk_per_trade": cfg["risk_per_trade"],
        "max_portfolio_exposure": cfg["max_portfolio_exposure"],
    }
    _insert_brain_snapshot(dsn, config_id, snapshot)

    # --------------------------------------------------------
    # מחזירים את ה- BrainSettings
    # --------------------------------------------------------
    return BrainSettings(
        mode=mode,
        risk_per_trade=cfg["risk_per_trade"],
        max_portfolio_exposure=cfg["max_portfolio_exposure"],
        max_notional_pct_hard=MAX_NOTIONAL_PCT_HARD_DEFAULT,
        atr_k_sl=ATR_K_SL,
        r1_R=R1_R,
        r2_R=R2_R,
        p1_pct=P1_PCT,
        p2_PCT=P2_PCT,
        p2_pct=P2_PCT,
        be_after_R=BE_AFTER_R,
        trail_atr_k=cfg["trail_atr_k"],
        max_bars_in_trade=MAX_BARS_IN_TRADE,
        blocked_symbols=blocked_symbols,
    )
