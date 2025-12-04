# bot/run_live_week.py
# ------------------------------------------------------------
# Trading bot (weekly loop) with:
# - Safe key filtering for DonchianTrendADXRSI / TradeManager
# - Standardized OHLCV (no KeyError('high'))
# - Support for CCXT (e.g. Bybit) and Alpaca
# - Basic TP/SL, time exit and equity logging
# - Brain integration (analyzer_v2):
#     * dynamic risk_per_trade / exposure / SL-TP parameters
#     * blocked symbols
#     * hard cap per-position notional
# - Hard filter: never trade stable/stable pairs (USDC/USD, USDT/USDC, etc.)
# ------------------------------------------------------------

import os
import sys
import math
import time
import csv as _csv
import inspect
from datetime import datetime, timezone

import yaml
import pandas as pd
from dotenv import load_dotenv

import ccxt

from bot.safety import guard_open  # currently not used but kept for future rules

try:
    from bot.monitor import start_heartbeat
except Exception:
    def start_heartbeat(*args, **kwargs):
        return None

# Timezone (לשעות מסחר)
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

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
from bot.analyzer_v2 import get_brain_settings, BrainSettings

try:
    from bot.connectors.alpaca_connector import AlpacaConnector
except Exception:
    AlpacaConnector = None

LOG_DIR = os.path.join(THIS_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_curve.csv")


# ------------------------
# CSV / misc utils
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


def get_min_amount_from_market(market: dict) -> float:
    try:
        lims = (market or {}).get("limits", {}) or {}
        amt = (lims.get("amount") or {}).get("min")
        return float(amt) if amt is not None else 0.0
    except Exception:
        return 0.0


def normalize_position_qty(conn, symbol: str, qty: float) -> float:
    if qty <= 0:
        return 0.0

    min_qty = 0.0
    step = 0.0

    try:
        ex = getattr(conn, "exchange", None) if conn is not None else None
        if ex is not None:
            market = ex.market(symbol)
            step = determine_amount_step(market)
            min_qty = get_min_amount_from_market(market)
    except Exception:
        pass

    if step > 0:
        qty = round_step(qty, step)

    threshold = min_qty if min_qty > 0 else 1e-8
    if qty < threshold:
        return 0.0

    return qty


# ------------------------
# Stable-pair helpers (avoid USDC/USD, USDT/USDC וכו')
# ------------------------
STABLE_TOKENS = {"USD", "USDT", "USDC", "USDG"}


def _extract_base_quote(symbol: str):
    """
    Tries to infer base/quote from a symbol:
    - 'BTC/USDT'        -> ('BTC', 'USDT')
    - 'BTC/USDT:USDT'   -> ('BTC', 'USDT')
    - 'USDTUSD'         -> ('USDT', 'USD')
    Returns (base, quote) or (None, None) if cannot parse.
    """
    if not symbol:
        return None, None

    core = symbol.upper()
    core = core.split(":")[0]  # remove futures suffix if exists

    if "/" in core:
        base, quote = core.split("/", 1)
        return base.strip(), quote.strip()

    # no '/', guess by stable suffix
    for q in sorted(STABLE_TOKENS, key=len, reverse=True):
        if core.endswith(q):
            base = core[: -len(q)]
            base = base.strip()
            if base:
                return base, q
    return None, None


def is_stable_pair_symbol(symbol: str) -> bool:
    """
    Returns True if BOTH sides of the pair are stablecoins / USD.
    """
    base, quote = _extract_base_quote(symbol)
    if not base or not quote:
        return False
    return base in STABLE_TOKENS and quote in STABLE_TOKENS


# ------------------------
# Equity helpers (Bybit & Alpaca)
# ------------------------
def fetch_bybit_equity() -> float:
    """
    Fetch equity from Bybit (USDT, UNIFIED/SPOT). Returns 0.0 on any error.
    """
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    if not api_key or not api_secret:
        print("⚠️ BYBIT_API_KEY/SECRET missing – returning 0.0 for Bybit equity")
        return 0.0

    try:
        exchange = ccxt.bybit(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )

        equity_unified = 0.0
        equity_spot = 0.0

        try:
            bal_u = exchange.fetch_balance({"type": "UNIFIED"})
            usdt_u = bal_u.get("USDT") or {}
            equity_unified = float(usdt_u.get("total") or usdt_u.get("free") or 0.0)
        except Exception as e_u:
            print(f"⚠️ fetch_balance UNIFIED failed: {e_u}")

        try:
            bal_s = exchange.fetch_balance()
            usdt_s = bal_s.get("USDT") or {}
            equity_spot = float(usdt_s.get("total") or usdt_s.get("free") or 0.0)
        except Exception as e_s:
            print(f"⚠️ fetch_balance SPOT failed: {e_s}")

        equity = max(equity_unified, equity_spot)
        print(
            f"💰 Bybit equity – UNIFIED={equity_unified} USDT, "
            f"SPOT={equity_spot} USDT, used={equity}"
        )
        return max(equity, 0.0)
    except Exception as e:
        print(f"⚠️ failed to fetch live balance from Bybit, returning 0.0. Error: {e}")
        return 0.0


def fetch_alpaca_equity() -> float:
    """
    Fetch equity from Alpaca using either APCA_* or ALPACA_* env vars.
    Returns 0.0 on any error.
    """
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET_KEY")
    base_url = (
        os.getenv("APCA_API_BASE_URL")
        or os.getenv("ALPACA_BASE_URL")
        or ""
    )

    if not key or not secret:
        print("⚠️ APCA/ALPACA API key/secret missing – returning 0.0 for Alpaca equity")
        return 0.0

    if not base_url:
        is_paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
        if is_paper:
            base_url = "https://paper-api.alpaca.markets"
        else:
            base_url = "https://api.alpaca.markets"

    try:
        try:
            import requests
        except ImportError:
            print("⚠️ 'requests' not installed – cannot fetch Alpaca equity, returning 0.0")
            return 0.0

        url = base_url.rstrip("/") + "/v2/account"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        eq_str = data.get("equity") or data.get("portfolio_value") or "0"
        equity = float(eq_str)
        print(f"💰 Alpaca equity – {equity}")
        return max(equity, 0.0)
    except Exception as e:
        print(f"⚠️ failed to fetch Alpaca equity, returning 0.0. Error: {e}")
        return 0.0


def fetch_alpaca_buying_power() -> float:
    """
    Fetch buying_power from Alpaca /v2/account.
    Returns 0.0 on any error.
    """
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET_KEY")
    base_url = (
        os.getenv("APCA_API_BASE_URL")
        or os.getenv("ALPACA_BASE_URL")
        or ""
    )

    if not key or not secret:
        print("⚠️ APCA/ALPACA API key/secret missing – returning 0.0 for Alpaca buying_power")
        return 0.0

    if not base_url:
        is_paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
        if is_paper:
            base_url = "https://paper-api.alpaca.markets"
        else:
            base_url = "https://api.alpaca.markets"

    try:
        import requests
    except ImportError:
        print("⚠️ 'requests' not installed – cannot fetch Alpaca buying_power, returning 0.0")
        return 0.0

    try:
        url = base_url.rstrip("/") + "/v2/account"
        headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        bp_str = data.get("buying_power") or "0"
        buying_power = float(bp_str)
        print(f"💰 Alpaca buying_power – {buying_power}")
        return max(buying_power, 0.0)
    except Exception as e:
        print(f"⚠️ failed to fetch Alpaca buying_power, returning 0.0. Error: {e}")
        return 0.0


# ------------------------
# OHLCV standardization
# ------------------------
def standardize_ohlcv(df_raw, symbol: str = ""):
    """
    Ensures DataFrame with columns: open, high, low, close [, volume].
    Supports:
      - list/tuple of OHLCV rows (ccxt-style)
      - DataFrame with different column names (o/h/l/c/v, etc.)
    """
    if df_raw is None:
        return None

    if not isinstance(df_raw, pd.DataFrame):
        if not df_raw:
            return None
        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(df_raw, columns=cols[: len(df_raw[0])])
    else:
        df = df_raw.copy()

    if df.empty:
        return None

    lower_cols = {c.lower(): c for c in df.columns}
    rename_map = {}

    mapping = {
        "open": ["open", "o"],
        "high": ["high", "h"],
        "low":  ["low", "l"],
        "close": ["close", "c"],
        "volume": ["volume", "v"],
    }

    for target, candidates in mapping.items():
        for cand in candidates:
            if cand in lower_cols:
                rename_map[lower_cols[cand]] = target
                break

    if rename_map:
        df = df.rename(columns=rename_map)

    if "time" in df.columns:
        df = df.set_index("time")
    elif "timestamp" in df.columns:
        df["time"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
        df = df.set_index("time")

    required = {"open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"standardize_ohlcv: missing required columns {missing}")

    cols_out = ["open", "high", "low", "close"]
    if "volume" in df.columns:
        cols_out.append("volume")

    df = df[cols_out].dropna(how="any")
    if df.empty:
        return None

    return df


# ------------------------
# Live position helpers
# ------------------------
def get_live_position_qty(conn, symbol: str, side: str):
    """
    Try to fetch real position size:
    - first via futures positions
    - then via spot balance (base asset)
    Returns (qty, market_type) where market_type in {"future", "spot"}.
    """
    if conn is None or not hasattr(conn, "exchange"):
        return None, "spot"

    ex = conn.exchange

    # futures
    try:
        positions = ex.fetch_positions([symbol])
        target_side = "long" if side == "long" else "short"
        for p in positions:
            if p.get("symbol") != symbol:
                continue
            p_side = (p.get("side") or p.get("positionSide") or "").lower()
            if p_side and p_side != target_side:
                continue
            raw = p.get("contracts") or p.get("size") or p.get("amount")
            if raw is None:
                continue
            qty = abs(float(raw))
            if qty > 0:
                return qty, "future"
    except Exception:
        pass

    # spot
    try:
        market = ex.market(symbol)
        base = market.get("base") or symbol.split("/")[0]
        balance = ex.fetch_balance()
        base_info = balance.get(base) or {}
        raw = base_info.get("free") or base_info.get("total") or 0
        qty = float(raw)
        return max(qty, 0.0), "spot"
    except Exception:
        return None, "spot"


def compute_close_qty(conn, symbol: str, side: str, requested_qty: float, fallback_qty: float):
    if requested_qty <= 0:
        return 0.0, fallback_qty

    live_qty, _ = get_live_position_qty(conn, symbol, side)
    if live_qty is None:
        live_qty = fallback_qty

    live_qty = max(live_qty or 0.0, 0.0)
    if live_qty <= 0:
        return 0.0, live_qty

    close_qty = min(requested_qty, live_qty)
    if close_qty <= 0:
        return 0.0, live_qty

    return close_qty, live_qty


# ------------------------
# Orders
# ------------------------
def place_order(conn, symbol: str, side: str, qty: float, reduce_only: bool = False):
    """
    Market order:
      - Alpaca via AlpacaConnector.create_market_order
      - CCXT via exchange.create_order
    side: "buy"/"sell"
    """
    # Alpaca path
    try:
        if AlpacaConnector is not None and isinstance(conn, AlpacaConnector):
            try:
                order = conn.create_market_order(symbol, side, qty)
                order_id = None
                if isinstance(order, dict):
                    order_id = order.get("id") or order.get("order_id")
                if not order_id:
                    order_id = str(order)
                print(f"[ORDER OK][ALPACA] {symbol} {side} {qty} => {order_id}")
                return order_id
            except Exception as e:
                print(f"[ORDER ERROR][ALPACA] {symbol} {side} {qty}: {e}")
                return None
    except NameError:
        pass

    # CCXT path
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
    feats: pd.DataFrame,
    ltf_df: pd.DataFrame,
    donchian_len: int,
) -> pd.DataFrame:
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
    rows_trades.append(
        [
            now_utc.isoformat(),
            connector,
            symbol,
            event_type,
            side,
            f"{price:.8f}",
            f"{qty:.8f}",
            "" if pnl is None else f"{pnl:.2f}",
            f"{equity:.2f}",
        ]
    )


# ------------------------
# Market hours helpers (חוקת "הזמנות כשהבורסה סגורה v1")
# ------------------------
def is_alpaca_equity_symbol(symbol: str) -> bool:
    """
    באלפאקה:
    - מניות/ETF = סימבול בלי '/'
    - קריפטו = BTC/USD, ETH/USD וכו'
    (הקובץ עובד עם שמות כמו 'ETH/USD' בקונפיג, אז זה פשוט.)
    """
    return "/" not in symbol


def is_equity_market_open(now_utc: datetime, market_tz) -> bool:
    """
    בודק אם שוק המניות פתוח לפי זמן מקומי:
    ימים: שני–שישי
    שעות: 09:30–16:00 (זמן ניו-יורק, בקירוב)
    """
    local = now_utc.astimezone(market_tz)
    if local.weekday() >= 5:  # 5=Saturday, 6=Sunday
        return False

    open_dt = local.replace(hour=9, minute=30, second=0, microsecond=0)
    close_dt = local.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_dt <= local <= close_dt


# ------------------------
# main
# ------------------------
def main():
    hb_thread = start_heartbeat()

    load_dotenv()
    with open(os.path.join(THIS_DIR, "config.yml"), "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Session rules (חוקת הזמנות כשהבורסה סגורה v1)
    session_rules = cfg.get("session_rules", {}) or {}
    market_tz_name = session_rules.get("timezone", "America/New_York")
    if ZoneInfo is not None:
        try:
            market_tz = ZoneInfo(market_tz_name)
        except Exception:
            print(f"[WARN] failed to load timezone '{market_tz_name}', fallback to UTC")
            market_tz = timezone.utc
    else:
        market_tz = timezone.utc

    equities_market_hours_only = bool(
        session_rules.get("equities_market_hours_only", False)
    )

    # DB (optional)
    db = None
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        try:
            db = DB(database_url)
        except Exception as e:
            print(f"[WARN] DB init failed: {e}")
            db = None

    # Brain state
    brain_settings: BrainSettings | None = None
    blocked_symbols: set[str] = set()
    max_notional_pct_hard: float = 0.20  # fallback, המוח יכול לעדכן
    last_brain_update_ts: float = 0.0

    # Strategy & trade manager (safe kwargs)
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

    # Portfolio & equity
    portfolio = cfg.get("portfolio", {}) or {}
    equity_cfg = portfolio.get("equity0", "auto")

    if isinstance(equity_cfg, str) and equity_cfg.lower() == "auto":
        equity_bybit = fetch_bybit_equity()
        equity_alpaca = fetch_alpaca_equity()
        equity = max(equity_bybit, equity_alpaca)
        print(
            f"💰 Auto equity – Bybit={equity_bybit:.2f}, Alpaca={equity_alpaca:.2f}, "
            f"used={equity:.2f}"
        )
        if equity <= 0:
            print("⚠️ Auto equity is 0 – bot will not open new positions until balance is > 0")
    else:
        try:
            equity = float(equity_cfg)
        except Exception:
            equity = 0.0

    # RiskManager (הערכים יתעדכנו מהמוח)
    rm = RiskManager(
        equity=equity,
        risk_per_trade=float(portfolio.get("risk_per_trade", 0.03)),
        max_position_pct=float(portfolio.get("max_position_pct", 1.0)),
    )

    config_id = os.getenv("BOT_CONFIG_ID", "SAFE_V1")

    # initial equity log
    now_utc = datetime.now(timezone.utc)
    write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}")])
    if db:
        try:
            db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
        except Exception as e:
            print(f"[WARN] DB write_equity init failed: {e}")

    # Connectors
    conns: list[tuple[dict, object]] = []
    live_connectors = cfg.get("live_connectors", []) or []
    for c in live_connectors:
        ctype = c.get("type", "ccxt")

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

        try:
            conn.init()
        except Exception as e:
            print(f"❌ init() failed for connector {c.get('name','?')}: {repr(e)}")
            continue

        if ctype == "ccxt":
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
            # Hard guard: never trade stable/stable pairs even if בקונפיג
            valid_syms = [s for s in valid_syms if not is_stable_pair_symbol(s)]

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
        else:
            requested_syms = list(c.get("symbols", []) or [])
            # Hard guard: never trade stable/stable pairs even if בקונפיג
            valid_syms = [s for s in requested_syms if not is_stable_pair_symbol(s)]

            print(
                f"✅ Alpaca connector '{c.get('name','alpaca')}' using {len(valid_syms)} symbols from config."
            )

        c_local = dict(c)
        c_local["symbols"] = valid_syms
        conns.append((c_local, conn))

    # init trades CSV header
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

    while True:
        # --- Brain update (every ~5 minutes) ---
        if db:
            try:
                now_ts = time.time()
                if now_ts - last_brain_update_ts > 300:
                    bs = get_brain_settings(config_id=config_id)
                    brain_settings = bs
                    last_brain_update_ts = now_ts

                    # החלת פרמטרים מהמוח
                    rm.risk_per_trade = bs.risk_per_trade
                    rm.max_position_pct = bs.max_portfolio_exposure

                    tm.atr_k_sl = bs.atr_k_sl
                    tm.r1_R = bs.r1_R
                    tm.r2_R = bs.r2_R
                    tm.p1_pct = bs.p1_pct
                    tm.p2_pct = bs.p2_pct
                    tm.be_after_R = bs.be_after_R
                    tm.trail_atr_k = bs.trail_atr_k
                    tm.max_bars_in_trade = bs.max_bars_in_trade

                    blocked_symbols = set(bs.blocked_symbols or set())
                    max_notional_pct_hard = bs.max_notional_pct_hard

                    print(
                        f"[BRAIN] mode={bs.mode} "
                        f"risk_per_trade={bs.risk_per_trade:.4f} "
                        f"max_exposure={bs.max_portfolio_exposure:.2f} "
                        f"blocked={len(blocked_symbols)}"
                    )
            except Exception as e:
                print(f"[WARN] brain update failed: {e}")

        now_utc = datetime.now(timezone.utc)
        rows_trades: list[list] = []
        snapshots: dict = {}

        # Alpaca buying_power – אם יש בכלל קונקטור אלפקה
        has_alpaca = any(c_cfg.get("type") == "alpaca" for c_cfg, _ in conns)
        alpaca_buying_power = fetch_alpaca_buying_power() if has_alpaca else 0.0

        # ---------------- fetch & features ----------------
        for c_cfg, conn in conns:
            tf = c_cfg.get("timeframe", "1m")
            htf = c_cfg.get("htf_timeframe", "5m")
            for sym in c_cfg.get("symbols", []):
                # בטיחות נוספת: גם כאן לא ניגשים בכלל לזוגות stable/stable
                if is_stable_pair_symbol(sym):
                    continue
                try:
                    ltf_df_raw = conn.fetch_ohlcv(sym, tf, limit=200)
                    htf_df_raw = conn.fetch_ohlcv(sym, htf, limit=200)

                    ltf_df = standardize_ohlcv(ltf_df_raw, sym)
                    htf_df = standardize_ohlcv(htf_df_raw, sym)

                    if ltf_df is None or htf_df is None:
                        continue

                    feats = prepare_features(ltf_df, htf_df, strat, donchian_len_cfg)
                    if feats.empty:
                        continue

                    last = feats.iloc[-1]
                    key = (c_cfg.get("name", "ccxt"), sym)
                    snapshots[key] = last
                except Exception as e:
                    print(f"⏭️ skip {sym}: {repr(e)}")
                    continue

        # check new bar
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

            write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}")])
            if db:
                try:
                    db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
                except Exception as e:
                    print(f"[WARN] DB write_equity loop failed: {e}")
            continue

        # ---------------- manage existing positions ----------------
        to_close = []
        for key, pos in list(open_positions.items()):  # key = (connector_name, symbol)
            row = snapshots.get(key)
            if row is None:
                continue

            price = float(row["close"])
            atr_now = float(row["atr"]) if pd.notna(row["atr"]) else None
            side = pos["side"]
            entry = pos["entry"]
            qty = pos["qty"]
            R = pos["R"]
            conn = pos.get("conn")

            # trailing SL (דינמי לפי המוח)
            if atr_now:
                k_trail = getattr(tm, "trail_atr_k", 1.2)
                trail = (
                    price - k_trail * atr_now
                    if side == "long"
                    else price + k_trail * atr_now
                )
                if side == "long":
                    pos["sl"] = max(pos["sl"], trail)
                else:
                    pos["sl"] = min(pos["sl"], trail)

            # move SL to BE after certain R
            if not pos["moved_to_be"] and atr_now:
                be_after_R = getattr(tm, "be_after_R", 0.8)
                if side == "long" and price >= entry + be_after_R * R:
                    pos["sl"] = max(pos["sl"], entry)
                    pos["moved_to_be"] = True
                if side == "short" and price <= entry - be_after_R * R:
                    pos["sl"] = min(pos["sl"], entry)
                    pos["moved_to_be"] = True

            # TP1
            if (not pos["tp1_done"]) and (
                (side == "long" and price >= pos["tp1"])
                or (side == "short" and price <= pos["tp1"])
            ):
                requested_close_qty = qty * getattr(tm, "p1_pct", 0.5)
                exit_side = "sell" if side == "long" else "buy"

                close_qty, live_qty = compute_close_qty(
                    conn, key[1], side, requested_close_qty, pos["qty"]
                )

                min_qty = 0.0
                if conn is not None and hasattr(conn, "exchange"):
                    try:
                        market = conn.exchange.market(key[1])
                        min_qty = get_min_amount_from_market(market)
                    except Exception as e:
                        print(f"[TP1] failed to get min_qty for {key}: {e}")

                if (
                    live_qty is not None
                    and live_qty > 0
                    and min_qty > 0
                    and live_qty < min_qty
                ):
                    print(
                        f"[TP1] dust on {key}: live_qty {live_qty} < min_qty {min_qty} – marking closed"
                    )
                    pos["qty"] = 0.0
                    pos["tp1_done"] = True
                    pos["tp2_done"] = True
                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=price,
                                realized_pnl=pos.get("realized_pnl", 0.0),
                                exit_type="DUST",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (DUST-TP1) failed for {key}: {e}")
                    to_close.append(key)
                    continue

                if close_qty <= 0:
                    print(f"[TP1] no qty to close for {key}")
                else:
                    order_id = None
                    if conn is not None:
                        order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)

                    if not order_id:
                        print(f"[TP1] order failed for {key}")
                    else:
                        pnl = (
                            (price - entry) * close_qty
                            if side == "long"
                            else (entry - price) * close_qty
                        )
                        equity += pnl
                        pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + pnl

                        new_qty_raw = max(
                            0.0,
                            (live_qty if live_qty is not None else pos["qty"]) - close_qty,
                        )
                        pos["qty"] = normalize_position_qty(conn, key[1], new_qty_raw)
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

            # TP2
            if (not pos["tp2_done"]) and (
                (side == "long" and price >= pos["tp2"])
                or (side == "short" and price <= pos["tp2"])
            ):
                requested_close_qty = pos["qty"] * getattr(tm, "p2_pct", 0.5)
                exit_side = "sell" if side == "long" else "buy"

                close_qty, live_qty = compute_close_qty(
                    conn, key[1], side, requested_close_qty, pos["qty"]
                )

                min_qty = 0.0
                if conn is not None and hasattr(conn, "exchange"):
                    try:
                        market = conn.exchange.market(key[1])
                        min_qty = get_min_amount_from_market(market)
                    except Exception as e:
                        print(f"[TP2] failed to get min_qty for {key}: {e}")

                if (
                    live_qty is not None
                    and live_qty > 0
                    and min_qty > 0
                    and live_qty < min_qty
                ):
                    print(
                        f"[TP2] dust on {key}: live_qty {live_qty} < min_qty {min_qty} – marking closed"
                    )
                    pos["qty"] = 0.0
                    pos["tp2_done"] = True
                    pos["tp1_done"] = True
                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=price,
                                realized_pnl=pos.get("realized_pnl", 0.0),
                                exit_type="DUST",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (DUST-TP2) failed for {key}: {e}")
                    to_close.append(key)
                    continue

                if close_qty <= 0:
                    print(f"[TP2] no qty to close for {key}")
                else:
                    order_id = None
                    if conn is not None:
                        order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)

                    if not order_id:
                        print(f"[TP2] order failed for {key}")
                    else:
                        pnl = (
                            (price - entry) * close_qty
                            if side == "long"
                            else (entry - price) * close_qty
                        )
                        equity += pnl
                        pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + pnl

                        new_qty_raw = max(
                            0.0,
                            (live_qty if live_qty is not None else pos["qty"]) - close_qty,
                        )
                        pos["qty"] = normalize_position_qty(conn, key[1], new_qty_raw)
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

            # SL
            if (side == "long" and price <= pos["sl"]) or (
                side == "short" and price >= pos["sl"]
            ):
                exit_side = "sell" if side == "long" else "buy"
                requested_close_qty = pos["qty"]

                close_qty, live_qty = compute_close_qty(
                    conn, key[1], side, requested_close_qty, pos["qty"]
                )

                min_qty = 0.0
                if conn is not None and hasattr(conn, "exchange"):
                    try:
                        market = conn.exchange.market(key[1])
                        min_qty = get_min_amount_from_market(market)
                    except Exception as e:
                        print(f"[SL] failed to get min_qty for {key}: {e}")

                if (
                    live_qty is not None
                    and live_qty > 0
                    and min_qty > 0
                    and live_qty < min_qty
                ):
                    print(
                        f"[SL] dust on {key}: live_qty {live_qty} < min_qty {min_qty} – marking closed"
                    )
                    pos["qty"] = 0.0
                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=pos["sl"],
                                realized_pnl=pos.get("realized_pnl", 0.0),
                                exit_type="DUST",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (DUST-SL) failed for {key}: {e}")
                    to_close.append(key)
                    continue

                if close_qty <= 0 or conn is None:
                    print(f"[SL] no qty to close for {key}")
                    order_id = None
                else:
                    order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)

                if not order_id:
                    print(f"[SL] order failed for {key}")
                else:
                    price_exit = pos["sl"]
                    pnl = (
                        (price_exit - entry) * close_qty
                        if side == "long"
                        else (entry - price_exit) * close_qty
                    )
                    equity += pnl
                    pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + pnl

                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=price_exit,
                                realized_pnl=pos.get("realized_pnl", pnl),
                                exit_type="SL",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (SL) failed for {key}: {e}")

                    new_qty_raw = max(
                        0.0,
                        (live_qty if live_qty is not None else pos["qty"]) - close_qty,
                    )
                    pos["qty"] = normalize_position_qty(conn, key[1], new_qty_raw)

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

            # TIME exit
            pos["bars"] += 1
            max_bars_in_trade = getattr(tm, "max_bars_in_trade", 48)
            if pos["bars"] >= max_bars_in_trade and not pos["tp2_done"]:
                exit_side = "sell" if side == "long" else "buy"
                requested_close_qty = pos["qty"]

                close_qty, live_qty = compute_close_qty(
                    conn, key[1], side, requested_close_qty, pos["qty"]
                )

                min_qty = 0.0
                if conn is not None and hasattr(conn, "exchange"):
                    try:
                        market = conn.exchange.market(key[1])
                        min_qty = get_min_amount_from_market(market)
                    except Exception as e:
                        print(f"[TIME] failed to get min_qty for {key}: {e}")

                if (
                    live_qty is not None
                    and live_qty > 0
                    and min_qty > 0
                    and live_qty < min_qty
                ):
                    print(
                        f"[TIME] dust on {key}: live_qty {live_qty} < min_qty {min_qty} – marking closed"
                    )
                    pos["qty"] = 0.0
                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=price,
                                realized_pnl=pos.get("realized_pnl", 0.0),
                                exit_type="DUST",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (DUST-TIME) failed for {key}: {e}")
                    to_close.append(key)
                    continue

                if close_qty <= 0 or conn is None:
                    print(f"[TIME] no qty to close for {key}")
                    order_id = None
                else:
                    order_id = place_order(conn, key[1], exit_side, close_qty, reduce_only=True)

                if not order_id:
                    print(f"[TIME] order failed for {key}")
                else:
                    exit_price = price
                    pnl = (
                        (exit_price - entry) * close_qty
                        if side == "long"
                        else (entry - exit_price) * close_qty
                    )
                    equity += pnl
                    pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + pnl

                    trade_id = pos.get("trade_id")
                    if trade_id and db and hasattr(db, "close_live_trade"):
                        try:
                            db.close_live_trade(
                                trade_id=trade_id,
                                exit_price=exit_price,
                                realized_pnl=pos.get("realized_pnl", pnl),
                                exit_type="TIME",
                                equity_at_exit=equity,
                            )
                        except Exception as e:
                            print(f"[WARN] close_live_trade (TIME) failed for {key}: {e}")

                    new_qty_raw = max(
                        0.0,
                        (live_qty if live_qty is not None else pos["qty"]) - close_qty,
                    )
                    pos["qty"] = normalize_position_qty(conn, key[1], new_qty_raw)

                    append_trade(
                        rows_trades,
                        now_utc,
                        key[0],
                        key[1],
                        "TIME",
                        side,
                        exit_price,
                        close_qty,
                        pnl,
                        equity,
                    )

                    to_close.append(key)

        for key in to_close:
            open_positions.pop(key, None)

        # exposure
        portfolio_notional = 0.0
        for key2, pos2 in open_positions.items():
            row2 = snapshots.get(key2)
            if row2 is None:
                continue
            price_now = float(row2["close"])
            portfolio_notional += pos2["qty"] * price_now

        max_portfolio_notional = equity * rm.max_position_pct
        remaining_notional = max(0.0, max_portfolio_notional - portfolio_notional)

        # ---------------- new entries ----------------
        for c_cfg, conn in conns:
            for sym in c_cfg.get("symbols", []):
                key = (c_cfg.get("name", "ccxt"), sym)

                # Hard guard: לעולם לא לפתוח טרייד על stable/stable
                if is_stable_pair_symbol(sym):
                    continue

                # Brain: do not open new trades on blocked symbols
                if sym in blocked_symbols:
                    continue

                if remaining_notional <= 0:
                    continue

                if key in open_positions:
                    continue
                if cooldowns.get(key, 0) > 0:
                    cooldowns[key] = max(0, cooldowns.get(key, 0) - 1)
                    continue

                # חוקת "הזמנות כשהבורסה סגורה":
                # אם מדובר באלפאקה + מניה/ETF – לא פותחים טרייד מחוץ לשעות המסחר.
                ctype = c_cfg.get("type", "ccxt")
                if (
                    ctype == "alpaca"
                    and equities_market_hours_only
                    and is_alpaca_equity_symbol(sym)
                ):
                    if not is_equity_market_open(now_utc, market_tz):
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

                atr_k_sl = getattr(tm, "atr_k_sl", 1.5)
                sl = (
                    price - atr_k_sl * atr_now
                    if side == "long"
                    else price + atr_k_sl * atr_now
                )
                R = (price - sl) if side == "long" else (sl - price)
                if R <= 0:
                    continue

                if ctype == "alpaca":
                    market = {}
                    step = 1.0
                    min_qty = None
                    min_cost = None

                    # מגבלה לפי buying_power אמיתי מאלפאקה
                    if alpaca_buying_power > 0:
                        qty_cap_bp = alpaca_buying_power / max(price, 1e-9)
                    else:
                        qty_cap_bp = float("inf")
                else:
                    market = {}
                    try:
                        market = conn.exchange.market(sym)
                    except Exception:
                        pass

                    step = determine_amount_step(market)
                    lims = (market or {}).get("limits", {}) or {}
                    min_qty = (lims.get("amount") or {}).get("min")
                    min_cost = (lims.get("cost") or {}).get("min")

                    # בבורסות אחרות אין לנו buying_power, אז אין מגבלה נוספת מהסוג הזה
                    qty_cap_bp = float("inf")

                qty_risk = (equity * rm.risk_per_trade) / max(R, 1e-12)
                qty_cap_equity = (equity * rm.max_position_pct) / max(price, 1e-9)
                qty_cap_remaining = remaining_notional / max(price, 1e-9)

                # Hard cap per position (פתרון טסלה) – למשל 20% מהתיק
                hard_notional_cap = max_notional_pct_hard * equity
                qty_cap_hard = hard_notional_cap / max(price, 1e-9)

                qty_raw = max(
                    0.0,
                    min(
                        qty_risk,
                        qty_cap_equity,
                        qty_cap_remaining,
                        qty_cap_bp,
                        qty_cap_hard,
                    ),
                )
                qty = round_step(qty_raw, step)

                if (min_qty is not None) and (qty < float(min_qty)):
                    qty = round_step(float(min_qty), step)

                notional = qty * price
                if (min_cost is not None) and (notional < float(min_cost)):
                    needed_qty = float(min_cost) / max(price, 1e-9)
                    qty = round_step(max(qty, needed_qty), step)

                if qty <= 0:
                    continue

                r1_R = getattr(tm, "r1_R", 1.0)
                r2_R = getattr(tm, "r2_R", 2.5)

                tp1 = (
                    price + r1_R * R
                    if side == "long"
                    else price - r1_R * R
                )
                tp2 = (
                    price + r2_R * R
                    if side == "long"
                    else price - r2_R * R
                )

                order_side = "buy" if side == "long" else "sell"
                order_id = place_order(conn, sym, order_side, qty, reduce_only=False)
                if not order_id:
                    print(f"[ENTER] order failed for {key}")
                    continue

                risk_usd = R * qty

                trade_id = None
                if db and hasattr(db, "open_live_trade"):
                    try:
                        trade_id = db.open_live_trade(
                            connector=key[0],
                            symbol=sym,
                            side=side,
                            entry_price=price,
                            qty=qty,
                            risk_usd=risk_usd,
                            equity_at_entry=equity,
                            config_id=config_id,
                        )
                    except Exception as e:
                        print(f"[WARN] open_live_trade failed for {key}: {e}")

                entry_notional = qty * price
                remaining_notional = max(0.0, remaining_notional - entry_notional)

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
                    "trade_id": trade_id,
                    "realized_pnl": 0.0,
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

        write_csv(EQUITY_CSV, ["time", "equity"], [[now_utc.isoformat(), f"{equity:.2f}")])
        if db:
            try:
                db.write_equity({"time": now_utc.isoformat(), "equity": float(f"{equity:.2f}")})
            except Exception as e:
                print(f"[WARN] DB write_equity loop failed: {e}")

        if time.time() - start_time >= SECONDS_IN_WEEK:
            break

        time.sleep(30)


if __name__ == "__main__":
    main()
