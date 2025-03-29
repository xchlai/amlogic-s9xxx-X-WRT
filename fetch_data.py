import ccxt
import pandas as pd
import time
from datetime import datetime, timedelta

# -------------------------------
# 1. Initialize Binance exchange via ccxt
# -------------------------------
exchange = ccxt.binance()
exchange.load_markets()

# -------------------------------
# 2. Get all coins with USDT trading pairs from Binance
# -------------------------------
usdt_pairs = [symbol for symbol in exchange.markets.keys() if symbol.endswith('/USDT')]
print(f"[DEBUG] Found {len(usdt_pairs)} USDT pairs on Binance.")

# -------------------------------
# 3. Set parameters for OHLCV fetching
# -------------------------------
timeframe = '15m'
limit = 1000  # Binance max data points per request
timeframe_ms = 15 * 60 * 1000  # 15 minutes in milliseconds

# -------------------------------
# 4. Loop over each USDT trading pair and fetch OHLCV data
# -------------------------------
for symbol in usdt_pairs:
    print(f"[DEBUG] Starting data fetch for {symbol}...")
    all_data = []
    # Use data from 10 years ago for a long historical period
    start_time = datetime.utcnow() - timedelta(days=365 * 10)
    since = exchange.parse8601(start_time.strftime('%Y-%m-%dT%H:%M:%SZ'))
    now = exchange.milliseconds()

    while since < now:
        try:
            print(f"[DEBUG] Fetching data for {symbol} from timestamp {since}...")
            data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
            if not data:
                print(f"[DEBUG] No more data returned for {symbol} at timestamp {since}. Breaking loop.")
                break
            all_data += data
            since = data[-1][0] + timeframe_ms
            print(f"[DEBUG] Fetched {len(all_data)} candles for {symbol} so far. Next 'since' set to {since}.")
            # Respect Binance rate limit
            time.sleep(exchange.rateLimit / 1000)
        except Exception as e:
            print(f"[DEBUG] Error fetching data for {symbol}: {e}. Retrying after 60 seconds...")
            time.sleep(60)  # wait a minute before retrying

    # -------------------------------
    # 5. Save the data to a CSV file if any data was fetched
    # -------------------------------
    if all_data:
        df = pd.DataFrame(all_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        filename = symbol.lower().replace('/', '_') + ".csv"
        df.to_csv(filename)
        print(f"[DEBUG] Saved data for {symbol} to {filename}. Total candles: {len(df)}.")
    else:
        print(f"[DEBUG] No data available for {symbol}.")
