# bot/log_writer.py
import os, csv, datetime

LOG_DIR = os.getenv("LOG_DIR", "/data/logs_live")
TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
os.makedirs(LOG_DIR, exist_ok=True)

_HEADERS = ["ts","env","symbol","event","side","price","qty","pnl","eq","exchange_order_id"]

def _init_file():
    if not os.path.exists(TRADES_CSV) or os.path.getsize(TRADES_CSV) == 0:
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(_HEADERS)

_init_file()

def write_confirmed_trade(*, env:str, symbol:str, event:str, side:str,
                          price:float, qty:float, pnl=None, eq=None, exchange_order_id:str=""):
    """
    כותב שורה רק אם יש exchange_order_id (כלומר הזמנה אושרה ע״י הבורסה).
    """
    if not exchange_order_id:
        return  # אל תכתוב בלי אישור אמיתי
    ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    row = [ts, env, symbol, event, side, price, qty, pnl if pnl is not None else "", eq if eq is not None else "", exchange_order_id]
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)
