import os
import ccxt

alpaca = ccxt.alpaca({
    "apiKey": os.getenv("ALPACA_API_KEY_ID"),
    "secret": os.getenv("ALPACA_API_SECRET_KEY"),
})

use_paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
if use_paper:
    alpaca.set_sandbox_mode(True)

base_url = os.getenv("ALPACA_BASE_URL")
if base_url:
    alpaca.urls["api"] = base_url

print(">>> Fetching balance...")
print(alpaca.fetch_balance())

print("\n>>> Fetching markets sample...")
markets = alpaca.fetch_markets()
print([m["symbol"] for m in markets[:10]])

print("\n>>> Fetching OHLCV for AAPL/USD (if market open)...")
try:
    ohlcv = alpaca.fetch_ohlcv("AAPL/USD", timeframe="1m", limit=5)
    for c in ohlcv:
        print(c)
except Exception as e:
    print("Error fetching OHLCV:", e)
