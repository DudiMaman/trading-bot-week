import os
from datetime import datetime, timezone
from typing import Any, Mapping

"""
DB helper עם fallback:
- מנסה psycopg (v3). אם אין/נכשל, מנסה psycopg2 (v2).
- אם גם זה נכשל, עובד במצב No-Op (CSV בלבד) כדי לא להפיל את הבוט.
- יוצר סכימה (טבלאות) אם לא קיימות.
"""

# =========================
# Connector label (single source of truth)
# =========================
def _as_bool(v):
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


_IS_TESTNET = _as_bool(os.getenv("BYBIT_TESTNET")) or _as_bool(os.getenv("TESTNET"))
# ניתן לעקוף ידנית דרך ENV: CONNECTOR_LABEL=bybit
_CONNECTOR_LABEL = os.getenv("CONNECTOR_LABEL") or ("bybit_testnet" if _IS_TESTNET else "bybit")


def connector_label() -> str:
    return _CONNECTOR_LABEL


def _normalize_trade_rows(rows):
    """
    מקבל iterable של רשומות טריידים ומחזיר list של tuples בסדר העמודות:
    (time, connector, symbol, type, side, price, qty, pnl, equity)
    - אם התקבל dict — נרכיב ממנו tuple לפי שמות השדות.
    - אם התקבל tuple/list — נחליף את האיבר השני (connector) בתווית האחידה.
    """
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append(
                (
                    r.get("time"),
                    connector_label(),
                    r.get("symbol"),
                    r.get("type"),
                    r.get("side"),
                    r.get("price"),
                    r.get("qty"),
                    r.get("pnl"),
                    r.get("equity"),
                )
            )
        else:
            # tuple/list
            lst = list(r)
            if len(lst) < 9:
                # חסר/שגוי – מדלגים בשקט
                continue
            lst[1] = connector_label()
            out.append(tuple(lst))
    return out


def _resolve_equity_value(e: Mapping[str, Any]) -> float:
    """
    מקבל dict בסגנון {"time": ..., "equity": ...} שמגיע מהבוט,
    ומחזיר equity לשמירה ב-DB.

    ברירת המחדל: מנסה להביא totalEquity חי מ-Bybit (באמצעות bot/live_equity.py).
    אם יש תקלה / אין מודול – נופל חזרה לערך שמגיע מהאירוע עצמו.
    """
    try:
        # import דינמי כדי לא להפיל את המודול אם live_equity לא קיים עדיין
        from .live_equity import get_live_total_equity

        live_equity = float(get_live_total_equity())
        print(
            f"[DB] overriding payload equity={e.get('equity')} "
            f"with live Bybit totalEquity={live_equity}"
        )
        return live_equity
    except Exception as err:
        payload_equity = float(e["equity"])
        print(
            f"[DB] live_equity fallback to payload equity={payload_equity}. "
            f"Reason: {err}"
        )
        return payload_equity


# -------------------------
# No-Op (CSV only)
# -------------------------
class _NoOpDB:
    def __init__(self, *args, **kwargs):
        err = kwargs.get("err")
        print(f"[DB] Disabled (CSV-only). Reason: {err}")

    # סכימה
    def ensure_schema(self):
        pass

    # מצב ריצה
    def get_state(self) -> str:
        return "RUNNING"

    def set_state(self, state: str):
        pass

    # כתיבה
    def write_trades(self, rows):
        pass

    def write_equity(self, e):
        pass

    # טריידים מאוחדים (live_trades)
    def open_live_trade(
        self,
        connector: str,
        symbol: str,
        side: str,
        entry_price: float,
        qty: float,
        risk_usd: float,
        equity_at_entry: float | None = None,
        config_id: str | None = None,
    ) -> int | None:
        return None

    def close_live_trade(
        self,
        trade_id: int,
        exit_price: float,
        realized_pnl: float,
        exit_type: str | None = None,
        equity_at_exit: float | None = None,
    ):
        pass

    def close(self):
        pass


# -------------------------
# psycopg v3
# -------------------------
def _make_psycopg_db(conn_str):
    import psycopg  # v3

    class _DB:
        def __init__(self, dsn):
            self.conn = psycopg.connect(dsn)
            self.conn.autocommit = True
            self.ensure_schema()

        def ensure_schema(self):
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists trades(
                      time timestamptz not null,
                      connector text,
                      symbol text,
                      type text,
                      side text,
                      price double precision,
                      qty double precision,
                      pnl double precision,
                      equity double precision
                    );
                """
                )
                cur.execute(
                    """
                    create table if not exists equity_curve(
                      time timestamptz primary key,
                      equity double precision
                    );
                """
                )
                cur.execute(
                    """
                    create table if not exists bot_state(
                      id int primary key default 1,
                      state text not null default 'RUNNING',
                      updated_at timestamptz not null default now()
                    );
                """
                )
                # ודא שקיימת שורה יחידה
                cur.execute(
                    "insert into bot_state (id) values (1) on conflict (id) do nothing;"
                )

                # טבלת טריידים מאוחדים (כניסה+יציאה)
                cur.execute(
                    """
                    create table if not exists live_trades(
                      id              bigserial primary key,
                      connector       text        not null,
                      symbol          text        not null,
                      side            text        not null,
                      time_entry      timestamptz not null,
                      time_exit       timestamptz,
                      entry_price     double precision not null,
                      exit_price      double precision,
                      qty             double precision not null,
                      equity_at_entry double precision,
                      equity_at_exit  double precision,
                      risk_usd        double precision not null,
                      realized_pnl    double precision,
                      r_multiple      double precision,
                      exit_type       text,
                      config_id       text
                    );
                """
                )

        def get_state(self) -> str:
            with self.conn.cursor() as cur:
                cur.execute("select state from bot_state where id=1;")
                row = cur.fetchone()
            return (row[0] if row else "RUNNING") or "RUNNING"

        def set_state(self, state: str):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into bot_state (id, state, updated_at) values (1, %s, now()) "
                    "on conflict (id) do update set state=excluded.state, updated_at=now();",
                    (state,),
                )

        # -------- live_trades: פתיחת טרייד --------
        def open_live_trade(
            self,
            connector: str,
            symbol: str,
            side: str,
            entry_price: float,
            qty: float,
            risk_usd: float,
            equity_at_entry: float | None = None,
            config_id: str | None = None,
        ) -> int:
            time_entry = datetime.now(timezone.utc)
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    insert into live_trades (
                      connector, symbol, side,
                      time_entry, entry_price, qty,
                      equity_at_entry, risk_usd, config_id
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning id;
                    """,
                    (
                        connector_label(),  # שומר תמיד תווית אחידה
                        symbol,
                        side,
                        time_entry,
                        float(entry_price),
                        float(qty),
                        float(equity_at_entry) if equity_at_entry is not None else None,
                        float(risk_usd),
                        config_id,
                    ),
                )
                row = cur.fetchone()
            return int(row[0])

        # -------- live_trades: סגירת טרייד --------
        def close_live_trade(
            self,
            trade_id: int,
            exit_price: float,
            realized_pnl: float,
            exit_type: str | None = None,
            equity_at_exit: float | None = None,
        ):
            with self.conn.cursor() as cur:
                cur.execute(
                    "select risk_usd from live_trades where id=%s;",
                    (trade_id,),
                )
                row = cur.fetchone()
                risk_usd = float(row[0]) if row and row[0] is not None else 0.0

                if risk_usd > 0:
                    r_multiple = float(realized_pnl) / risk_usd
                else:
                    r_multiple = None

                cur.execute(
                    """
                    update live_trades
                    set time_exit      = %s,
                        exit_price     = %s,
                        realized_pnl   = %s,
                        r_multiple     = %s,
                        exit_type      = %s,
                        equity_at_exit = %s
                    where id = %s;
                    """,
                    (
                        datetime.now(timezone.utc),
                        float(exit_price),
                        float(realized_pnl),
                        r_multiple,
                        exit_type,
                        float(equity_at_exit) if equity_at_exit is not None else None,
                        trade_id,
                    ),
                )

        def write_trades(self, rows):
            normalized = _normalize_trade_rows(rows)
            if not normalized:
                return
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into trades
                      (time, connector, symbol, type, side, price, qty, pnl, equity)
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    normalized,
                )

        def write_equity(self, e):
            equity_value = _resolve_equity_value(e)
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s) "
                    "on conflict (time) do update set equity=excluded.equity;",
                    (e["time"], equity_value),
                )

        def close(self):
            try:
                self.conn.close()
            except Exception:
                pass

    return _DB(conn_str)


# -------------------------
# psycopg2 v2
# -------------------------
def _make_psycopg2_db(conn_str):
    import psycopg2  # v2

    class _DB:
        def __init__(self, dsn):
            self.conn = psycopg2.connect(dsn)
            self.conn.autocommit = True
            self.ensure_schema()

        def ensure_schema(self):
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists trades(
                      time timestamptz not null,
                      connector text,
                      symbol text,
                      type text,
                      side text,
                      price double precision,
                      qty double precision,
                      pnl double precision,
                      equity double precision
                    );
                """
                )
                cur.execute(
                    """
                    create table if not exists equity_curve(
                      time timestamptz primary key,
                      equity double precision
                    );
                """
                )
                cur.execute(
                    """
                    create table if not exists bot_state(
                      id int primary key default 1,
                      state text not null default 'RUNNING',
                      updated_at timestamptz not null default now()
                    );
                """
                )
                cur.execute(
                    "insert into bot_state (id) values (1) on conflict (id) do nothing;"
                )

                cur.execute(
                    """
                    create table if not exists live_trades(
                      id              bigserial primary key,
                      connector       text        not null,
                      symbol          text        not null,
                      side            text        not null,
                      time_entry      timestamptz not null,
                      time_exit       timestamptz,
                      entry_price     double precision not null,
                      exit_price      double precision,
                      qty             double precision not null,
                      equity_at_entry double precision,
                      equity_at_exit  double precision,
                      risk_usd        double precision not null,
                      realized_pnl    double precision,
                      r_multiple      double precision,
                      exit_type       text,
                      config_id       text
                    );
                """
                )

        def get_state(self) -> str:
            with self.conn.cursor() as cur:
                cur.execute("select state from bot_state where id=1;")
                row = cur.fetchone()
            return (row[0] if row else "RUNNING") or "RUNNING"

        def set_state(self, state: str):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into bot_state (id, state, updated_at) values (1, %s, now()) "
                    "on conflict (id) do update set state=excluded.state, updated_at=now();",
                    (state,),
                )

        def open_live_trade(
            self,
            connector: str,
            symbol: str,
            side: str,
            entry_price: float,
            qty: float,
            risk_usd: float,
            equity_at_entry: float | None = None,
            config_id: str | None = None,
        ) -> int:
            time_entry = datetime.now(timezone.utc)
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    insert into live_trades (
                      connector, symbol, side,
                      time_entry, entry_price, qty,
                      equity_at_entry, risk_usd, config_id
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning id;
                    """,
                    (
                        connector_label(),
                        symbol,
                        side,
                        time_entry,
                        float(entry_price),
                        float(qty),
                        float(equity_at_entry) if equity_at_entry is not None else None,
                        float(risk_usd),
                        config_id,
                    ),
                )
                row = cur.fetchone()
            return int(row[0])

        def close_live_trade(
            self,
            trade_id: int,
            exit_price: float,
            realized_pnl: float,
            exit_type: str | None = None,
            equity_at_exit: float | None = None,
        ):
            with self.conn.cursor() as cur:
                cur.execute(
                    "select risk_usd from live_trades where id=%s;",
                    (trade_id,),
                )
                row = cur.fetchone()
                risk_usd = float(row[0]) if row and row[0] is not None else 0.0

                if risk_usd > 0:
                    r_multiple = float(realized_pnl) / risk_usd
                else:
                    r_multiple = None

                cur.execute(
                    """
                    update live_trades
                    set time_exit      = %s,
                        exit_price     = %s,
                        realized_pnl   = %s,
                        r_multiple     = %s,
                        exit_type      = %s,
                        equity_at_exit = %s
                    where id = %s;
                    """,
                    (
                        datetime.now(timezone.utc),
                        float(exit_price),
                        float(realized_pnl),
                        r_multiple,
                        exit_type,
                        float(equity_at_exit) if equity_at_exit is not None else None,
                        trade_id,
                    ),
                )

        def write_trades(self, rows):
            normalized = _normalize_trade_rows(rows)
            if not normalized:
                return
            with self.conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into trades
                      (time, connector, symbol, type, side, price, qty, pnl, equity)
                    values
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    normalized,
                )

        def write_equity(self, e):
            equity_value = _resolve_equity_value(e)
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s) "
                    "on conflict (time) do update set equity=excluded.equity;",
                    (e["time"], equity_value),
                )

        def close(self):
            try:
                self.conn.close()
            except Exception:
                pass

    return _DB(conn_str)


# -------------------------
# Factory
# -------------------------
class DB:
    def __new__(cls, dsn: str | None):
        if not dsn:
            return _NoOpDB(err="No DATABASE_URL set")
        try:
            return _make_psycopg_db(dsn)
        except Exception as e_psycopg:
            try:
                return _make_psycopg2_db(dsn)
            except Exception as e_psycopg2:
                return _NoOpDB(
                    err=f"psycopg error: {e_psycopg}; psycopg2 error: {e_psycopg2}"
                )
