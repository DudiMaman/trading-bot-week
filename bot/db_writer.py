# bot/db_writer.py
import os
from datetime import datetime, timezone

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
            out.append((
                r.get("time"),
                connector_label(),
                r.get("symbol"),
                r.get("type"),
                r.get("side"),
                r.get("price"),
                r.get("qty"),
                r.get("pnl"),
                r.get("equity"),
            ))
        else:
            # tuple/list
            lst = list(r)
            if len(lst) < 9:
                # חסר/שגוי – מדלגים בשקט
                continue
            lst[1] = connector_label()
            out.append(tuple(lst))
    return out


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
                cur.execute("""
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
                """)
                cur.execute("""
                    create table if not exists equity_curve(
                      time timestamptz primary key,
                      equity double precision
                    );
                """)
                cur.execute("""
                    create table if not exists bot_state(
                      id int primary key default 1,
                      state text not null default 'RUNNING',
                      updated_at timestamptz not null default now()
                    );
                """)
                # ודא שקיימת שורה יחידה
                cur.execute("insert into bot_state (id) values (1) on conflict (id) do nothing;")

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
                    (state,)
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
                    normalized
                )

        def write_equity(self, e):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s) "
                    "on conflict (time) do update set equity=excluded.equity;",
                    (e["time"], e["equity"])
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
                cur.execute("""
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
                """)
                cur.execute("""
                    create table if not exists equity_curve(
                      time timestamptz primary key,
                      equity double precision
                    );
                """)
                cur.execute("""
                    create table if not exists bot_state(
                      id int primary key default 1,
                      state text not null default 'RUNNING',
                      updated_at timestamptz not null default now()
                    );
                """)
                cur.execute("insert into bot_state (id) values (1) on conflict (id) do nothing;")

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
                    (state,)
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
                    normalized
                )

        def write_equity(self, e):
            with self.conn.cursor() as cur:
                cur.execute(
                    "insert into equity_curve (time, equity) values (%s, %s) "
                    "on conflict (time) do update set equity=excluded.equity;",
                    (e["time"], e["equity"])
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
                return _NoOpDB(err=f"psycopg error: {e_psycopg}; psycopg2 error: {e_psycopg2}")
