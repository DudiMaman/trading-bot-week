# bot/run_live_week.py
# ------------------------------------------------------------
# Trading bot (weekly loop) with:
# - Safe key filtering for DonchianTrendADXRSI / TradeManager
# - AUTO symbol discovery (Bybit via CCXT)
# - Valid symbol filtering + min-qty/min-notional/precision
# - Fallback signal logic (Donchian breakout) if strategy yields no signals
# - Robust try/except and dual logging (CSV + optional Postgres)
#
# ⚠️ WARNING: this version sends REAL MARKET ORDERS to Bybit using your API keys.
# Use at your own risk and only with money you can afford to lose.
# ------------------------------------------------------------

import os
import sys
import math
import time
import yaml
import csv as _csv
import inspect
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import ccxt

from bot.safety import guard_open

try:
    from bot.monitor import start_heartbeat
except Exception:
    # בלם בטיחות: אם יש בעיה ב-monitor, לא מפילים את ה-worker
    def start_heartbeat(*args, **kwargs):
        return None

THIS_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.dirname(THIS_DIR)
for p in (ROOT_DIR, THIS_DIR):
    if p not in sys.path:
        sys.path.append(p)

from bot.strategies import DonchianTrendADXRSI
from bot.risk import RiskManager, TradeManager
from bot.utils import atr as calc_atr
from bot.connectors.ccxt_connector import CCXTConnector
from bot.db_writer import DB

try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

LOG_DIR = os.path.join(THIS_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")


# ------------------------
# Utilities
# ------------------------
def write_csv(path: str, header: list[str], rows: list[list]):
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        w = _csv.writer(fh)
        if new_file:
            w.writerow(header)
        for r in rows:
            w.writerow(r)


def round_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step


def determine_amount_step(market: dict) -> float:
    step = 1e-6
    prec = (market or {}).get("precision") or {}
    if "amount" in prec and isinstance(prec["amount"], int):
        step = 10 ** (-prec["amount"])
    else:
        lim_amt = (market or {}).get("limits", {}).get("amount", {}) or {}
        step = float(lim_amt.get("step") or step)
    return max(step, 1e-12)


def place_order(conn, symbol: str, side: str, qty: float, reduce_only: bool = False):
    """
    מבצע פקודת מרקט אמיתית בבייביט דרך CCXT.
    side: "buy" / "sell"
    אם reduce_only=True – מיועד ליציאות (TP/SL/TIME) בפיוצ'רס.
    מחזיר order_id אם הצליח, אחרת None.
    """
    try:
        from ccxt.base.errors import ExchangeError
    except Exception:
        ExchangeError = Exception

    try:
        params = {}
        if reduce_only:
            params["reduceOnly"] = True

        order = conn.exchange.create_order(
            symbol=symbol,
            type="market",
            side=side,
            amount=qty,
            price=None,
            params=params,
        )
        order_id = order.get("id")
        print(f"[ORDER OK] {symbol} {side} {qty} => {order_id}")
        return order_id

    except ExchangeError as e:
        print(f"[ORDER ERROR] ExchangeError on {symbol} {side} {qty}: {e}")
    except Exception as e:
        print(f"[ORDER ERROR] {symbol} {side} {qty}: {repr(e)}")

    return None


def attach_atr(ltf_df: pd.DataFrame) -> pd.Series:
    return calc_atr(ltf_df, 14)


def ensure_signal_columns(
    feats: pd.DataFrame, ltf_df: pd.DataFrame, donchian_len: int
) -> pd.DataFrame:
    """
    Fallback: אם אין עמודות סיגנל, או שהכול False, נחשב סיגנל פריצה דונצ'יאן בסיסי.
    long_setup: close פורץ את max(high, N)
    short_setup: close יורד מתחת ל-min(low, N)
    """
    feats = feats.copy()
    need_fallback = False
    if "long_setup" not in feats.columns or "short_setup" not in feats.columns:
        need_fallback = True
    else:
        if (feats["long_setup"].sum() + feats["short_setup"].sum()) == 0:
            need_fallback = True

    if need_fallback:
        N = max(2, int(donchian_len or 4))
        highs = ltf_df["high"].rolling(N).max()
        lows = ltf_df["low"].rolling(N).min()
        close = ltf_df["close"]
        long_setup = close > highs.shift(1)
        short_setup = close < lows.shift(1)

        # התאמת האינדקס: נאחד על פי ה־index של feats
        tmp = pd.DataFrame(index=feats.index)
        tmp["long_setup"] = long_setup.reindex(feats.index).fillna(False)
        tmp["short_setup"] = short_setup.reindex(feats.index).fillna(False)
        feats["long_setup"] = tmp["long_setup"]
        feats["short_setup"] = tmp["short_setup"]

    return feats


def prepare_features(
    ltf_df: pd.DataFrame,
    htf_df: pd.DataFrame,
    strat: DonchianTrendADXRSI,
    donchian_len: int,
) -> pd.DataFrame:
    f = strat.prepare(ltf_df, htf_df)
    f["atr"] = attach_atr(ltf_df)
    f = ensure_signal_columns(f, ltf_df, donchian_len)
    return f


def append_trade(
    rows_trades: list[list],
    now_utc: datetime,
    connector: str,
    symbol: str,
    event_type: str,
    side: str,
    price: float,
    qty: float,
    pnl: float | None,
    equity: float,
):
    """
    עוטפת את הלוג של טרייד אחד, כדי שלא נכתוב את אותו rows_trades.append בכל מקום.
    """
    rows_trades.append(
        [
            now_utc.isoformat(),
            connector,
            symbol,
            event_type,  # ENTER / TP1 / TP2 / SL / TIME
            side,        # long / short
            f"{price:.8f}",
            f"{qty:.8f}",
            "" if pnl is None else f"{pnl:.2f}",
            f"{equity:.2f}",
        ]
    )


# ------------------------
# Main
# ------------------------
def main():
    hb_thread = start_heartbeat()

    # 1) Load env + config
    load_dotenv()
    with open(os.path.join(THIS_DIR, "config.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 2) Optional DB
    db = None
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            db = DB(database_url)
        except Exception as e:
            print(f"[WARN] DB init failed: {e}")
            db = None

    # 3) Strategy / TradeManager (safe filtering)
    raw_s = cfg.get("strategy", {}) or {}
    accepted_s = set(inspect.signature(DonchianTrendADXRSI).parameters.keys())
    clean_s = {k: v for k, v in raw_s.items() if k in accepted_s}
    donchian_len_cfg = int(raw_s.get("donchian_len", 4))
    unknown_s = sorted(set(raw_s.keys()) - accepted_s)
    if unknown_s:
        print(f"⚠️ Ignoring unknown strategy keys: {unknown_s}")
    strat = DonchianTrendADXRSI(**clean_s)

    raw_t = cfg.get("trade_manager", {}) or {}
    accepted_t = set(inspect.signature(TradeManager).parameters.keys())
    clean_t = {k: v for k, v in raw_t.items() if k in accepted_t}
    unknown_t = sorted(set(raw_t.keys()) - accepted_t)
    if unknown_t:
        print(f"⚠️ Ignoring unknown trade_manager keys: {unknown_t}")
    tm = TradeManager(**clean_t)

    # 4) Portfolio – משיכת equity מבייביט כשמוגדר "auto"
    portfolio = cfg.get("portfolio", {}) or {}
    equity_cfg = portfolio.get("equity0", "auto")

    if isinstance(equity_cfg, str) and equity_cfg.lower() == "auto":
        api_key = os.getenv("BYBIT_API_KEY")
        api_secret = os.getenv("BYBIT_API_SECRET")
        equity = 0.0
        try:
            if not api_key or not api_secret:
                print("⚠️ BYBIT_API_KEY/SECRET חסרים – מתחיל עם equity = 0")
            else:
                exchange = ccxt.bybit(
                    {
                        "apiKey": api_key,
                        "secret": api_secret,
                        "enableRateLimit": True,
                        "options": {
                            "defaultType": "swap",
                        },
                    }
                )
                balance = exchange.fetch_balance({"type": "UNIFIED"})
                usdt = balance.get("USDT") or {}
                equity = float(usdt.get("total") or usdt.get("free") or 0.0)
                print(f"💰 Live Bybit equity (USDT): {equity}")
        except Exception as e:
            print(f"⚠️ כשל בשליפת יתרה חיה מבייביט, מתחיל עם 0. שגיאה: {e}")
            equity = 0.0
    else:
        try:
            equity = float(equity_cfg)
        except Exception:
            equity = 0.0

    rm = RiskManager(
        equity=equity,
        risk_per_trade=float(portfolio.get("risk_per_trade", 0.03)),
        max_position_pct=float(portfolio.get("max_position_pct", 0.70)),
    )

    # 5) Initial equity log
    now_utc = datetime.now(timezone.utc)
    write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}"]])
    if db:
        try:
            db.write_equity(
                {"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")}
            )
        except Exception as e:
            print(f"[WARN] DB write_equity init failed: {e}")

    # 6) Connectors + AUTO
    conns: list[tuple[dict, object]] = []
    live_connectors = cfg.get("live_connectors", []) or []
    for c in live_connectors:
        ctype = c.get("type")
        if ctype == "ccxt":
            conn = CCXTConnector(
                c.get("exchange_id", "bybit"),
                paper=c.get("paper", True),
                default_type=c.get("default_type", "spot"),
            )
        elif ctype == "alpaca":
            if AlpacaConnector is None:
                print("ℹ️ Alpaca connector not available — skipping.")
                continue
            conn = AlpacaConnector(paper=c.get("paper", True))
        else:
            print(f"ℹ️ Unknown connector type '{ctype}' — skipping.")
            continue

        # init connector
        try:
            conn.init()
        except Exception as e:
            print(f"❌ init() failed for connector {c.get('name','?')}: {repr(e)}")
            continue

        # apply Bybit API keys from env (for real-money mode)
        api_key = os.getenv("BYBIT_API_KEY")
        api_secret = os.getenv("BYBIT_API_SECRET")
        if api_key and api_secret:
            try:
                conn.exchange.apiKey = api_key
                conn.exchange.secret = api_secret
            except Exception as e:
                print(f"[WARN] failed to set Bybit credentials on exchange: {e}")
        else:
            print("⚠️ BYBIT_API_KEY/BYBIT_API_SECRET not found in environment")

        # load markets
        try:
            markets = conn.exchange.load_markets()
        except Exception as e:
            print(f"❌ load_markets() failed: {e}")
            markets = {}

        requested_syms = list(c.get("symbols", []) or [])
        if "AUTO" in requested_syms:
            auto_syms = [
                m
                for m, info in markets.items()
                if info.get("quote") == "USDT" and info.get("active", True)
            ][:50]
            cfg_syms = requested_syms + auto_syms
        else:
            cfg_syms = requested_syms

        available = set(getattr(conn.exchange, "symbols", []) or [])
        valid_syms = [s for s in cfg_syms if s in available]

        if not valid_syms:
            print(
                f"⚠️ No valid symbols for connector '{c.get('name','ccxt')}'. "
                f"Requested={len(cfg_syms)}, Available={len(available)}"
            )
        else:
            print(
                f"✅ Connector '{c.get('name','ccxt')}' loaded {len(valid_syms)} valid symbols "
                f"(of {len(cfg_syms)} requested)."
            )

        c_local = dict(c)
        c_local["symbols"] = valid_syms
        conns.append((c_local, conn))

    # 7) Init CSV trades header
    write_csv(
        TRADES_CSV,
        ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"],
        [],
    )

    open_positions: dict = {}
    cooldowns: dict = {}
    last_bar_ts: dict = {}
    start_time = time.time()
    SECONDS_IN_WEEK = 7 * 24 * 60 * 60

    # 8) Main loop
    while True:
        now_utc = datetime.now(timezone.utc)
        rows_trades: list[list] = []
        snapshots: dict = {}

        # Fetch & features
        for c_cfg, conn in conns:
            tf = c_cfg.get("timeframe", "1m")
            htf = c_cfg.get("htf_timeframe", "5m")
            for sym in c_cfg.get("symbols", []):
                try:
                    ltf_df = conn.fetch_ohlcv(sym, tf, limit=600)
                    htf_df = conn.fetch_ohlcv(sym, htf, limit=600)
                    feats = prepare_features(ltf_df, htf_df, strat, donchian_len_cfg)
                    last = feats.iloc[-1]
                    key = (c_cfg.get("name", "ccxt"), sym)
                    snapshots[key] = last
                except Exception as e:
                    print(f"⏭️ skip {sym}: {repr(e)}")
                    continue

        progressed_any = False
        for key, row in snapshots.items():
            ts = row.name
            if last_bar_ts.get(key) != ts:
                last_bar_ts[key] = ts
                progressed_any = True

        if not progressed_any:
            time.sleep(15)
            if time.time() - start_time >= SECONDS_IN_WEEK:
                break
            write_csv(
                EQUITY_CSV,
                ["time", "equity"],
                [[now_utc.isoformat(), f"{equity:.2f}"]],
            )
            if db:
                try:
                    db.write_equity(
                        {"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")}
                    )
                except Exception as e:
                    print(f"[WARN] DB write_equity loop failed: {e}")
            continue

        # Manage positions (TP1 / TP2 / SL / TIME)
        to_close = []
        for key, pos in list(open_positions.items()):
            row = snapshots.get(key)
            if row is None:
                continue

            price = float(row["close"])
            atr_now = float(row["atr"]) if pd.notna(row["atr"]) else None
            side = pos["side"]          # "long" / "short"
            entry = pos["entry"]
            qty = pos["qty"]
            R = pos["R"]
            conn = pos.get("conn")      # CCXTConnector instance

            # trailing SL by ATR
            if atr_now:
                trail = tm.trail_level(side, price, atr_now, after_tp1=pos["tp1_done"])
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # move SL to BE after certain R
            if not pos["moved_to_be"] and atr_now:
                if side == "long" and price >= entry + tm.be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry)
                    pos["moved_to_be"] = True
                if side == "short" and price <= entry - tm.be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry)
                    pos["moved_to_be"] = True

            # ---- TP1 (partial close) ----
            if (not pos["tp1_done"]) and (
                (side == "long" and price >= pos["tp1"])
                or (side == "short" and price <= pos["tp1"])
            ):
                close_qty = qty * tm.p1_pct

                # real exit: reverse side, reduceOnly=True
                exit_side = "sell" if side == "long" else "buy"
                if conn is not None and close_qty > 0:
                    order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)
                else:
                    order_id = None

                if not order_id:
                    print(f"[TP1] order failed for {key}")
                else:
                    pnl = (
                        (price - entry) * close_qty
                        if side == "long"
                        else (entry - price) * close_qty
                    )
                    equity += pnl
                    pos["qty"] = qty - close_qty
                    pos["tp1_done"] = True

                    append_trade(
                        rows_trades,
                        now_utc,
                        key[0],
                        key[1],
                        "TP1",
                        side,
                        price,
                        close_qty,
                        pnl,
                        equity,
                    )

            # ---- TP2 (partial close / rest) ----
            if (not pos["tp2_done"]) and (
                (side == "long" and price >= pos["tp2"])
                or (side == "short" and price <= pos["tp2"])
            ):
                close_qty = pos["qty"] * tm.p2_pct

                exit_side = "sell" if side == "long" else "buy"
                if conn is not None and close_qty > 0:
                    order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)
                else:
                    order_id = None

                if not order_id:
                    print(f"[TP2] order failed for {key}")
                else:
                    pnl = (
                        (price - entry) * close_qty
                        if side == "long"
                        else (entry - price) * close_qty
                    )
                    equity += pnl
                    pos["qty"] = pos["qty"] - close_qty
                    pos["tp2_done"] = True

                    append_trade(
                        rows_trades,
                        now_utc,
                        key[0],
                        key[1],
                        "TP2",
                        side,
                        price,
                        close_qty,
                        pnl,
                        equity,
                    )

            # ---- SL (stop-loss exit of all remaining qty) ----
            if (side == "long" and price <= pos["sl"]) or (
                side == "short" and price >= pos["sl"]
            ):
                if pos["qty"] > 0 and conn is not None:
                    exit_side = "sell" if side == "long" else "buy"
                    order_id = place_order(
                        conn, key[1], exit_side, pos["qty"], reduce_only=True
                    )
                else:
                    order_id = None

                if not order_id:
                    print(f"[SL] order failed for {key}")
                else:
                    price_exit = pos["sl"]
                    close_qty = pos["qty"]
                    pnl = (
                        (price_exit - entry) * close_qty
                        if side == "long"
                        else (entry - price_exit) * close_qty
                    )
                    equity += pnl

                    append_trade(
                        rows_trades,
                        now_utc,
                        key[0],
                        key[1],
                        "SL",
                        side,
                        price_exit,
                        close_qty,
                        pnl,
                        equity,
                    )

                    to_close.append(key)

            # ---- TIME exit (if max bars reached and still open) ----
            pos["bars"] += 1
            if pos["bars"] >= tm.max_bars_in_trade and not pos["tp2_done"]:
                if pos["qty"] > 0 and conn is not None:
                    exit_side = "sell" if side == "long" else "buy"
                    order_id = place_order(
                        conn, key[1], exit_side, pos["qty"], reduce_only=True
                    )
                else:
                    order_id = None

                if not order_id:
                    print(f"[TIME] order failed for {key}")
                else:
                    close_qty = pos["qty"]
                    pnl = (
                        (price - entry) * close_qty
                        if side == "long"
                        else (entry - price) * close_qty
                    )
                    equity += pnl

                    append_trade(
                        rows_trades,
                        now_utc,
                        key[0],
                        key[1],
                        "TIME",
                        side,
                        price,
                        close_qty,
                        pnl,
                        equity,
                    )

                    to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        # Entries (open new positions – REAL MARKET ORDERS)
        for c_cfg, conn in conns:
            for sym in c_cfg.get("symbols", []):
                key = (c_cfg.get("name", "ccxt"), sym)
                if key in open_positions:
                    continue
                if cooldowns.get(key, 0) > 0:
                    cooldowns[key] = max(0, cooldowns.get(key, 0) - 1)
                    continue

                row = snapshots.get(key)
                if row is None or pd.isna(row.get("atr")) or row["atr"] <= 0:
                    continue

                sig = 1 if row.get("long_setup") else (-1 if row.get("short_setup") else 0)
                if sig == 0:
                    continue

                price = float(row["close"])
                atr_now = float(row["atr"])
                side = "long" if sig == 1 else "short"

                sl = (
                    price - tm.atr_k_sl * atr_now
                    if side == "long"
                    else price + tm.atr_k_sl * atr_now
                )
                R = (price - sl) if side == "long" else (sl - price)
                if R <= 0:
                    continue

                market = {}
                try:
                    market = conn.exchange.market(sym)
                except Exception:
                    pass

                step = determine_amount_step(market)
                lims = (market or {}).get("limits", {}) or {}
                min_qty = (lims.get("amount") or {}).get("min")
                min_cost = (lims.get("cost") or {}).get("min")

                qty_risk = (equity * rm.risk_per_trade) / max(R, 1e-12)
                qty_cap = (equity * rm.max_position_pct) / max(price, 1e-9)
                qty = max(0.0, min(qty_risk, qty_cap))
                qty = round_step(qty, step)

                if (min_qty is not None) and (qty < float(min_qty)):
                    qty = round_step(float(min_qty), step)

                notional = qty * price
                if (min_cost is not None) and (notional < float(min_cost)):
                    needed_qty = float(min_cost) / max(price, 1e-9)
                    qty = round_step(max(qty, needed_qty), step)

                if qty <= 0:
                    continue

                tp1 = (
                    price + tm.r1_R * R
                    if side == "long"
                    else price - tm.r1_R * R
                )
                tp2 = (
                    price + tm.r2_R * R
                    if side == "long"
                    else price - tm.r2_R * R
                )

                # REAL entry order
                order_side = "buy" if side == "long" else "sell"
                order_id = place_order(conn, sym, order_side, qty, reduce_only=False)
                if not order_id:
                    print(f"[ENTER] order failed for {key}")
                    continue

                open_positions[key] = {
                    "side": side,
                    "entry": price,
                    "sl": sl,
                    "tp1": tp1,
                    "tp2": tp2,
                    "qty": qty,
                    "R": R,
                    "bars": 0,
                    "tp1_done": False,
                    "tp2_done": False,
                    "moved_to_be": False,
                    "conn": conn,
                    "entry_order_id": order_id,
                }

                append_trade(
                    rows_trades,
                    now_utc,
                    key[0],
                    key[1],
                    "ENTER",
                    side,
                    price,
                    qty,
                    None,
                    equity,
                )

        if rows_trades:
            write_csv(
                TRADES_CSV,
                ["time", "connector", "symbol", "type", "side", "price", "qty", "pnl", "equity"],
                rows_trades,
            )
            if db:
                try:
                    db.write_trades(rows_trades)
                except Exception as e:
                    print(f"[WARN] DB write_trades failed: {e}")

        write_csv(
            EQUITY_CSV,
            ["time", "equity"],
            [[now_utc.isoformat(), f"{equity:.2f}"]],
        )
        if db:
            try:
                db.write_equity(
                    {"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")}
                )
            except Exception as e:
                print(f"[WARN] DB write_equity loop failed: {e}")

        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

        time.sleep(30)


if __name__ == "__main__":
    main()
