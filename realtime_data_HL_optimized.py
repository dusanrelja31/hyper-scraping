import asyncio
import json
import websockets
import sqlite3
import csv
import os
from datetime import datetime
import pytz
from hyperliquid.info import Info
from hyperliquid.utils import constants
from example_utils import load_config

# Define WebSocket URLs explicitly
MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# Real-time optimization settings
BATCH_SIZE = 50  # Reduced for faster writes
BUFFER_FLUSH_INTERVAL = 10  # Seconds
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_DELAY = 5

def init_db(coin):
    """Initialize SQLite database for a coin with orderbook, trades, candles, asset_context, and orderbook_depth tables."""
    coin_folder = f"data/{coin}"
    os.makedirs(coin_folder, exist_ok=True)
    
    db_path = os.path.join(coin_folder, f"{coin}_main_market_data.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS orderbook
                 (timestamp REAL, best_bid REAL, bid_volume REAL, best_ask REAL, ask_volume REAL, mid_price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (timestamp REAL, price REAL, volume REAL, side TEXT, notional REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS candles
                 (timestamp REAL, mark_price REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS asset_context
                 (timestamp REAL, oracle_price REAL, funding_rate REAL, open_interest REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS orderbook_depth
                 (timestamp REAL, level INTEGER, bid_price REAL, bid_volume REAL, ask_price REAL, ask_volume REAL)''')
    
    # Create indexes for faster queries
    c.execute('CREATE INDEX IF NOT EXISTS idx_orderbook_time ON orderbook(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_candles_time ON candles(timestamp)')
    
    conn.commit()
    conn.close()

def init_csv_files(coin):
    """Initialize CSV files for order book, trade, candle, asset context, and order book depth data."""
    coin_folder = f"data/{coin}"
    os.makedirs(coin_folder, exist_ok=True)
    
    with open(os.path.join(coin_folder, f"{coin}_main_orderbook.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Best Bid", "Bid Volume", "Best Ask", "Ask Volume", "Mid Price"])
    with open(os.path.join(coin_folder, f"{coin}_main_trades.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Price", "Volume", "Side", "Notional"])
    with open(os.path.join(coin_folder, f"{coin}_main_candles.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Mark Price"])
    with open(os.path.join(coin_folder, f"{coin}_main_asset_context.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Oracle Price", "Funding Rate", "Open Interest"])
    with open(os.path.join(coin_folder, f"{coin}_main_orderbook_depth.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Level", "Bid Price", "Bid Volume", "Ask Price", "Ask Volume"])

async def get_asset_id(coin, is_mainnet):
    """Retrieve the asset ID and exact coin name for a given coin."""
    api_url = constants.MAINNET_API_URL if is_mainnet else constants.TESTNET_API_URL
    info = Info(api_url, skip_ws=True)
    try:
        meta = info.meta()
        assets = meta.get("universe", [])
        if not assets:
            raise ValueError(f"No assets found in meta response for {coin}. Response: {meta}")
        for index, asset in enumerate(assets):
            if asset["name"].lower() == coin.lower():
                if asset.get("isDelisted", False):
                    raise ValueError(f"Asset '{coin}' is delisted.")
                return index, asset["name"]
        raise ValueError(f"Asset '{coin}' not found. Available assets: {[a['name'] for a in assets]}")
    except Exception as e:
        raise ValueError(f"Failed to fetch asset ID for '{coin}': {str(e)}")

async def send_ping(websocket, interval=30):
    """Send periodic ping messages to keep the WebSocket connection alive."""
    while True:
        try:
            await websocket.ping()
            print(f"[PING] Connection alive at {datetime.now().strftime('%H:%M:%S')}")
            await asyncio.sleep(interval)
        except Exception as e:
            print(f"[ERROR] Ping failed: {str(e)}")
            break

async def flush_buffers(coin, valid_coins, orderbook_rows, orderbook_depth_rows, trade_rows, candle_rows, asset_ctx_rows):
    """Flush all buffered data to database and CSV."""
    db_path = os.path.join(f"data/{coin}", f"{coin}_main_market_data.db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    try:
        if orderbook_rows[coin]:
            c.executemany("INSERT INTO orderbook VALUES (?, ?, ?, ?, ?, ?)", orderbook_rows[coin])
        if orderbook_depth_rows[coin]:
            c.executemany("INSERT INTO orderbook_depth VALUES (?, ?, ?, ?, ?, ?)", orderbook_depth_rows[coin])
        if trade_rows[coin]:
            c.executemany("INSERT INTO trades VALUES (?, ?, ?, ?, ?)", trade_rows[coin])
        if candle_rows[coin]:
            c.executemany("INSERT INTO candles VALUES (?, ?)", candle_rows[coin])
        if asset_ctx_rows[coin]:
            c.executemany("INSERT INTO asset_context VALUES (?, ?, ?, ?)", asset_ctx_rows[coin])
        
        conn.commit()
    finally:
        conn.close()
    
    # Clear buffers
    orderbook_rows[coin].clear()
    orderbook_depth_rows[coin].clear()
    trade_rows[coin].clear()
    candle_rows[coin].clear()
    asset_ctx_rows[coin].clear()

async def subscribe_to_data(coins=["BTC", "ETH", "HYPE"]):
    """Subscribe to order book, trade, candle, and asset context data for specified coins."""
    config = load_config()
    is_mainnet = config["is_mainnet"]
    ws_url = MAINNET_WS_URL if is_mainnet else TESTNET_WS_URL
    print(f"\n[INFO] Using WebSocket URL: {ws_url}")
    print(f"[INFO] Network: {'MAINNET' if is_mainnet else 'TESTNET'}")
    print(f"[INFO] Starting Real-Time Data Collection at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Validate coins and initialize storage
    valid_coins = []
    coin_asset_map = {}
    for coin in coins:
        try:
            asset_id, exact_coin = await get_asset_id(coin, is_mainnet)
            init_db(exact_coin)
            init_csv_files(exact_coin)
            valid_coins.append(exact_coin)
            coin_asset_map[exact_coin] = asset_id
            print(f"[✓] {exact_coin} validated (Asset ID: {asset_id})")
        except ValueError as e:
            print(f"[✗] Skipping {coin}: {e}")
            continue

    if not valid_coins:
        print("[ERROR] No valid coins to subscribe to. Exiting.")
        return

    orderbook_rows = {coin: [] for coin in valid_coins}
    orderbook_depth_rows = {coin: [] for coin in valid_coins}
    trade_rows = {coin: [] for coin in valid_coins}
    candle_rows = {coin: [] for coin in valid_coins}
    asset_ctx_rows = {coin: [] for coin in valid_coins}
    error_count = 0
    max_errors = 5
    reconnect_attempt = 0

    cest = pytz.timezone("Europe/Paris")
    last_flush_time = datetime.now()

    while reconnect_attempt < MAX_RECONNECT_ATTEMPTS:
        ping_task = None
        try:
            async with websockets.connect(ws_url, ping_interval=None) as websocket:
                reconnect_attempt = 0
                error_count = 0
                ping_task = asyncio.create_task(send_ping(websocket, interval=30))
                print(f"\n[✓] WebSocket connected at {datetime.now().strftime('%H:%M:%S')}")

                # Subscribe to all coins
                for coin in valid_coins:
                    subscriptions = [
                        {"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}},
                        {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}},
                        {"method": "subscribe", "subscription": {"type": "candle", "coin": coin, "interval": "1m"}},
                        {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}},
                    ]
                    for sub in subscriptions:
                        await websocket.send(json.dumps(sub))
                        await asyncio.sleep(0.1)
                    print(f"[→] Subscribed to {coin} (ID: {coin_asset_map[coin]})")

                print(f"\n[INFO] Listening for real-time updates...\n")

                while True:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=60)
                        data = json.loads(message)
                        cest_timestamp = datetime.now(cest).timestamp()

                        channel = data.get("channel")

                        if channel == "l2Book":
                            book_data = data.get("data", {})
                            coin = book_data.get("coin", "Unknown")
                            if coin not in valid_coins:
                                continue
                            levels = book_data.get("levels", [])
                            bids = levels[0] if len(levels) > 0 else []
                            asks = levels[1] if len(levels) > 1 else []
                            if bids and asks:
                                best_bid = float(bids[0]['px'])
                                best_ask = float(asks[0]['px'])
                                bid_volume = float(bids[0]['sz'])
                                ask_volume = float(asks[0]['sz'])
                                mid_price = (best_bid + best_ask) / 2
                                print(f"[BOOK] {coin} | Bid: {best_bid} ({bid_volume}) | Ask: {best_ask} ({ask_volume}) | Mid: {mid_price}")
                                orderbook_rows[coin].append((cest_timestamp, best_bid, bid_volume, best_ask, ask_volume, mid_price))
                                for i in range(min(5, len(bids), len(asks))):
                                    bid_price = float(bids[i]['px'])
                                    bid_volume = float(bids[i]['sz'])
                                    ask_price = float(asks[i]['px'])
                                    ask_volume = float(asks[i]['sz'])
                                    orderbook_depth_rows[coin].append((cest_timestamp, i+1, bid_price, bid_volume, ask_price, ask_volume))
                                with open(os.path.join(f"data/{coin}", f"{coin}_main_orderbook.csv"), "a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([cest_timestamp, best_bid, bid_volume, best_ask, ask_volume, mid_price])

                        elif channel == "trades":
                            trades = data.get("data", [])
                            coin = trades[0].get("coin", "Unknown") if trades else "Unknown"
                            if coin not in valid_coins:
                                continue
                            for trade in trades:
                                price = float(trade["px"])
                                volume = float(trade["sz"])
                                side = "Buy" if trade["side"] == "B" else "Sell"
                                notional = price * volume
                                print(f"[TRADE] {coin} | {side} {volume} @ {price} (Notional: {notional})")
                                trade_rows[coin].append((cest_timestamp, price, volume, side, notional))
                                with open(os.path.join(f"data/{coin}", f"{coin}_main_trades.csv"), "a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([cest_timestamp, price, volume, side, notional])

                        elif channel == "candle":
                            candle_data = data.get("data", {})
                            coin = candle_data.get("s", "Unknown")
                            if coin not in valid_coins:
                                continue
                            mark_price = float(candle_data.get("c", 0))
                            print(f"[CANDLE] {coin} | Mark Price: {mark_price}")
                            candle_rows[coin].append((cest_timestamp, mark_price))
                            with open(os.path.join(f"data/{coin}", f"{coin}_main_candles.csv"), "a", newline="") as f:
                                writer = csv.writer(f)
                                writer.writerow([cest_timestamp, mark_price])

                        elif channel == "activeAssetCtx":
                            ctx_data = data.get("data", {})
                            coin = ctx_data.get("coin", "Unknown")
                            if coin not in valid_coins:
                                continue
                            ctx = ctx_data.get("ctx", {})
                            try:
                                oracle_price = float(ctx.get("oraclePx", 0))
                                funding_rate = float(ctx.get("funding", 0))
                                open_interest = float(ctx.get("openInterest", 0))
                                print(f"[CTX] {coin} | Oracle: {oracle_price} | Funding: {funding_rate} | OI: {open_interest}")
                                asset_ctx_rows[coin].append((cest_timestamp, oracle_price, funding_rate, open_interest))
                                with open(os.path.join(f"data/{coin}", f"{coin}_main_asset_context.csv"), "a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([cest_timestamp, oracle_price, funding_rate, open_interest])
                            except (ValueError, TypeError) as e:
                                print(f"[ERROR] Failed to parse {coin} context: {str(e)}")
                                continue

                        elif channel == "subscriptionResponse":
                            print(f"[SUB] Subscription confirmed")

                        elif channel == "error":
                            print(f"[ERROR] Server error: {data.get('data', 'Unknown')}")
                            error_count += 1
                            if error_count >= max_errors:
                                print("[ERROR] Too many errors. Reconnecting...")
                                break

                        # Periodic flush to database
                        if (datetime.now() - last_flush_time).seconds >= BUFFER_FLUSH_INTERVAL:
                            for coin in valid_coins:
                                if orderbook_rows[coin] or trade_rows[coin] or candle_rows[coin]:
                                    await flush_buffers(coin, valid_coins, orderbook_rows, orderbook_depth_rows, trade_rows, candle_rows, asset_ctx_rows)
                            last_flush_time = datetime.now()
                            print(f"[FLUSH] Database updated at {last_flush_time.strftime('%H:%M:%S')}")

                    except asyncio.TimeoutError:
                        print("[WARNING] No data received for 60 seconds...")

                    except Exception as e:
                        print(f"[ERROR] Message processing failed: {str(e)}")
                        error_count += 1
                        if error_count >= max_errors:
                            break

        except websockets.exceptions.ConnectionClosed as e:
            reconnect_attempt += 1
            print(f"\n[DISCONNECT] Connection closed: {str(e)}")
            print(f"[INFO] Reconnection attempt {reconnect_attempt}/{MAX_RECONNECT_ATTEMPTS} in {RECONNECT_DELAY} seconds...\n")
            
            # Final flush before reconnect
            for coin in valid_coins:
                await flush_buffers(coin, valid_coins, orderbook_rows, orderbook_depth_rows, trade_rows, candle_rows, asset_ctx_rows)
            
            if ping_task:
                ping_task.cancel()
            await asyncio.sleep(RECONNECT_DELAY)

        except Exception as e:
            reconnect_attempt += 1
            print(f"\n[ERROR] Unexpected error: {str(e)}")
            print(f"[INFO] Reconnection attempt {reconnect_attempt}/{MAX_RECONNECT_ATTEMPTS} in {RECONNECT_DELAY} seconds...\n")
            
            # Final flush before reconnect
            for coin in valid_coins:
                await flush_buffers(coin, valid_coins, orderbook_rows, orderbook_depth_rows, trade_rows, candle_rows, asset_ctx_rows)
            
            if ping_task:
                ping_task.cancel()
            await asyncio.sleep(RECONNECT_DELAY)

    print("[ERROR] Max reconnection attempts reached. Exiting.")

def main():
    """Main function to run the data collection."""
    print("\n" + "="*50)
    print(" HYPERLIQUID REAL-TIME DATA COLLECTION")
    print("="*50)
    print("Press Ctrl+C to stop\n")
    try:
        asyncio.run(subscribe_to_data(coins=["BTC", "ETH", "HYPE"]))
    except KeyboardInterrupt:
        print("\n\n[INFO] Data collection stopped by user")
    except Exception as e:
        print(f"\n[ERROR] Fatal error: {e}")

if __name__ == "__main__":
    main()
