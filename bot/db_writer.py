import psycopg
from psycopg.rows import dict_row


class DB:
    """
    עטיפה פשוטה ל-Postgres:
    - טבלת equity
    - טבלת trades
    - טבלת live_trades
    - טבלת bot_settings להגדרות מוח גלובליות
    - טבלת symbol_overrides להגדרות פר-סימבול (למשל חסימת סימבולים)
    בלי לוגים מיותרים ובלי override נוסף של equity.
    """

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._ensure_schema()

    def _ensure_schema(self):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.cursor()

            # טבלת equity – צילום מצב ההון לאורך זמן
            cur.execute(
                """
                create table if not exists equity (
                    time timestamptz not null,
                    equity double precision not null
                );
                """
            )

            # טבלת trades – כל אירועי הטריידים (ENTER/TP/SL/TIME)
            cur.execute(
                """
                create table if not exists trades (
                    time timestamptz not null,
                    connector text not null,
                    symbol text not null,
                    type text not null,
                    side text not null,
                    price double precision not null,
                    qty double precision not null,
                    pnl double precision,
                    equity double precision
                );
                """
            )

            # טבלת live_trades – לניהול פוזיציות פתוחות/סגורות
            cur.execute(
                """
                create table if not exists live_trades (
                    id serial primary key,
                    connector text not null,
                    symbol text not null,
                    side text not null,
                    entry_price double precision not null,
                    qty double precision not null,
                    risk_usd double precision not null,
                    equity_at_entry double precision not null,
                    config_id text,
                    opened_at timestamptz not null default now(),
                    exit_price double precision,
                    realized_pnl double precision,
                    exit_type text,
                    equity_at_exit double precision,
                    closed_at timestamptz
                );
                """
            )

            # --- מיגרציה לסכמות ישנות של live_trades עם time_entry ---
            try:
                # 1) אם אין עמודת time_entry – ניצור אותה
                cur.execute(
                    """
                    alter table live_trades
                    add column if not exists time_entry timestamptz;
                    """
                )

                # 2) נגדיר ברירת מחדל now() לעמודה הזו
                cur.execute(
                    """
                    alter table live_trades
                    alter column time_entry set default now();
                    """
                )

                # 3) נמלא ערך לכל רשומה קיימת שבה time_entry הוא NULL
                cur.execute(
                    """
                    update live_trades
                    set time_entry = now()
                    where time_entry is null;
                    """
                )

                # 4) נוודא שהעמודה היא NOT NULL (כמו בסכמה הישנה שלך)
                cur.execute(
                    """
                    alter table live_trades
                    alter column time_entry set not null;
                    """
                )
            except Exception as e:
                # לא מפיל את האפליקציה אם המיגרציה נכשלה – רק מדפיס אזהרה
                print(f"[WARN] live_trades time_entry migration failed: {e}")

            # ------------------------
            # טבלת bot_settings – הגדרות מוח גלובליות
            # ------------------------
            cur.execute(
                """
                create table if not exists bot_settings (
                    key text primary key,
                    value jsonb not null,
                    updated_at timestamptz not null default now()
                );
                """
            )

            # לוודא שקיים updated_at גם אם הטבלה הייתה קיימת קודם
            cur.execute(
                """
                alter table bot_settings
                add column if not exists updated_at timestamptz not null default now();
                """
            )

            # ------------------------
            # טבלת symbol_overrides – הגדרות מוח פר-סימבול
            # ------------------------
            cur.execute(
                """
                create table if not exists symbol_overrides (
                    symbol text primary key,
                    block_new_trades boolean not null default false,
                    max_risk_per_trade double precision,
                    notes text,
                    updated_at timestamptz not null default now()
                );
                """
            )

            # מיגרציה רכה – אם הטבלה קיימת בלי אחד העמודות, נוסיף
            cur.execute(
                """
                alter table symbol_overrides
                add column if not exists block_new_trades boolean not null default false;
                """
            )
            cur.execute(
                """
                alter table symbol_overrides
                add column if not exists max_risk_per_trade double precision;
                """
            )
            cur.execute(
                """
                alter table symbol_overrides
                add column if not exists notes text;
                """
            )
            cur.execute(
                """
                alter table symbol_overrides
                add column if not exists updated_at timestamptz not null default now();
                """
            )

    # ------------------------
    # equity
    # ------------------------
    def write_equity(self, record: dict):
        """
        record דוגמה:
        {"time": "...", "equity": 36.45}
        לא עושה override ל-equity – משתמש בערך שמגיע מהבוט.
        """
        time = record.get("time")
        equity = float(record.get("equity") or 0.0)

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute(
                "insert into equity (time, equity) values (%s, %s);",
                (time, equity),
            )

    # ------------------------
    # trades
    # ------------------------
    def write_trades(self, rows: list[list]):
        """
        rows כמו ב-trades.csv:
        [time, connector, symbol, type, side, price, qty, pnl, equity]
        """
        if not rows:
            return

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                insert into trades
                (time, connector, symbol, type, side, price, qty, pnl, equity)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                [
                    (
                        r[0],          # time (isoformat string)
                        r[1],          # connector
                        r[2],          # symbol
                        r[3],          # type
                        r[4],          # side
                        float(r[5]) if r[5] != "" else 0.0,  # price
                        float(r[6]) if r[6] != "" else 0.0,  # qty
                        float(r[7]) if (len(r) > 7 and r[7] != "") else None,  # pnl
                        float(r[8]) if (len(r) > 8 and r[8] != "") else None,  # equity
                    )
                    for r in rows
                ],
            )

    # ------------------------
    # live trades (פתיחת/סגירת טרייד בזמן אמת)
    # ------------------------
    def open_live_trade(
        self,
        connector: str,
        symbol: str,
        side: str,
        entry_price: float,
        qty: float,
        risk_usd: float,
        equity_at_entry: float,
        config_id: str | None = None,
    ) -> int | None:
        """
        פותח רשומה ב-live_trades ומחזיר trade_id.
        time_entry מטופל על ידי ברירת המחדל של ה-DB (now()).
        """
        with psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                insert into live_trades
                (connector, symbol, side, entry_price, qty, risk_usd, equity_at_entry, config_id)
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                returning id;
                """,
                (
                    connector,
                    symbol,
                    side,
                    float(entry_price),
                    float(qty),
                    float(risk_usd),
                    float(equity_at_entry),
                    config_id,
                ),
            )
            row = cur.fetchone()
            return row["id"] if row else None

    def close_live_trade(
        self,
        trade_id: int,
        exit_price: float,
        realized_pnl: float,
        exit_type: str,
        equity_at_exit: float,
    ):
        """
        מעדכן טרייד פתוח ב-live_trades כסגור (SL/TP/TIME).
        """
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                update live_trades
                set
                    exit_price = %s,
                    realized_pnl = %s,
                    exit_type = %s,
                    equity_at_exit = %s,
                    closed_at = now()
                where id = %s;
                """,
                (
                    float(exit_price),
                    float(realized_pnl),
                    exit_type,
                    float(equity_at_exit),
                    int(trade_id),
                ),
            )

    # ------------------------
    # Brain helpers – קריאה להגדרות דינמיות
    # ------------------------
    def get_bot_settings(self) -> dict:
        """
        מחזיר dict של הגדרות גלובליות של הבוט מתוך bot_settings:
        {
          "risk_per_trade": 0.02,
          "something_else": ...
        }
        value הוא jsonb, כלומר יכול להיות מספר / מחרוזת / dict וכו'.
        """
        settings: dict = {}
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            cur.execute("select key, value from bot_settings;")
            rows = cur.fetchall() or []
            for row in rows:
                k = row["key"]
                v = row["value"]
                settings[k] = v
        return settings

    def get_symbol_overrides(self) -> dict:
        """
        מחזיר dict של הגדרות פר-סימבול מתוך symbol_overrides:
        {
          "TSLA": {
             "block_new_trades": True,
             "max_risk_per_trade": 0.01,
             "notes": "נחטף בבר קטן אחד",
             "updated_at": <datetime>
          },
          ...
        }
        """
        overrides: dict = {}
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                select symbol,
                       block_new_trades,
                       max_risk_per_trade,
                       notes,
                       updated_at
                from symbol_overrides;
                """
            )
            rows = cur.fetchall() or []
            for row in rows:
                symbol = row["symbol"]
                overrides[symbol] = {
                    "block_new_trades": row["block_new_trades"],
                    "max_risk_per_trade": row["max_risk_per_trade"],
                    "notes": row["notes"],
                    "updated_at": row["updated_at"],
                }
        return overrides
