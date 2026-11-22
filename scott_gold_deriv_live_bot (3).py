import asyncio
import csv
import json
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, time, timezone
import pytz
import websockets

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DERIV_APP_ID = 1089
DERIV_API_TOKEN = "pat_8fce0136be77a0d808c1262b34088263e231c3151aa517614c44bce11ee3af82"
DERIV_WS_URL = "".join(["wss", "://", "ws.", "derivws", f".com/websockets/v3?app_id={DERIV_APP_ID}"])

SYMBOL = "frxXAUUSD"       # Deriv Gold Symbol
STAKE_AMOUNT = 100.0       # Har trade par raqam (USD)
MULTIPLIER = 100           # Gold Multiplier
CANDLE_GRANULARITY = 300   # 5-Minute Candles
CSV_LOG_FILE = "gold_bot_performance_log.csv"


def log_print(msg):
    """Instant output print karta hai taake Windows CMD mein text buffer na ho."""
    print(msg, flush=True)


class DirectDerivClient:
    def __init__(self, url):
        self.url = url
        self.ws = None

    async def connect(self):
        headers = {
            "Origin": "https://app.deriv.com",
            "User-Agent": "Mozilla/5.0"
        }
        self.ws = await asyncio.wait_for(
            websockets.connect(self.url, additional_headers=headers),
            timeout=15
        )

    async def send_request(self, payload, timeout=15):
        await self.ws.send(json.dumps(payload))
        response = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        return json.loads(response)

    async def close(self):
        if self.ws:
            await self.ws.close()


class ScottGoldDerivBot:
    def __init__(self, atr_period=14, rvol_lookback=30, rvol_threshold=2.0, stake=100.0, multiplier=100):
        self.atr_period = atr_period
        self.rvol_lookback = rvol_lookback
        self.rvol_threshold = rvol_threshold
        self.stake = stake
        self.multiplier = multiplier
        self.active_zone = None
        self._init_csv_logger()

    def _init_csv_logger(self):
        if not os.path.exists(CSV_LOG_FILE):
            with open(CSV_LOG_FILE, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp_UTC", "Contract_ID", "Signal_Type", "Session",
                    "Entry_Spot", "Exit_Spot", "Stake_USD", "Stop_Loss_USD",
                    "Take_Profit_USD", "Net_PnL_USD", "Status", "Account_Balance_USD"
                ])

    def log_trade_to_csv(self, row_data):
        with open(CSV_LOG_FILE, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row_data)

    def calculate_indicators(self, df):
        df = df.copy()
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift(1))
        low_close = np.abs(df['low'] - df['close'].shift(1))

        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = tr.ewm(alpha=1 / self.atr_period, adjust=False).mean()

        df['avg_vol'] = df['volume'].rolling(window=self.rvol_lookback).mean().shift(1)
        df['rvol'] = df['volume'] / df['avg_vol']
        return df

    def is_tradable_time(self, current_dt_utc, current_rvol):
        central_tz = pytz.timezone('US/Central')
        ct_time = current_dt_utc.astimezone(central_tz).time()

        if time(8, 30) <= ct_time < time(10, 15):
            return True, "PRIME_SESSION"
        elif time(10, 15) <= ct_time < time(12, 30):
            if current_rvol >= 1.5:
                return True, "DEAD_ZONE_HIGH_VOL"
            return False, "DEAD_ZONE_SKIP"
        elif time(12, 30) <= ct_time < time(14, 50):
            return True, "AFTERNOON_SESSION"

        return False, "OUTSIDE_HOURS_OR_MOC"

    def detect_volume_event(self, row):
        if pd.notna(row['rvol']) and row['rvol'] >= self.rvol_threshold:
            self.active_zone = {
                'high': row['high'],
                'low': row['low'],
                'atr_at_event': row['atr'],
                'timestamp': row['timestamp'],
                'triggered': False
            }
            log_print(f"[EVENT ZONE DETECTED] Time: {row['timestamp']} | High: {row['high']} | Low: {row['low']} | RVOL: {row['rvol']:.2f}x")

    def check_signals(self, row):
        if not self.active_zone or self.active_zone['triggered']:
            return None

        tradable, session_status = self.is_tradable_time(row['timestamp'], row['rvol'])
        if not tradable:
            return None

        z_high = self.active_zone['high']
        z_low = self.active_zone['low']
        atr = self.active_zone['atr_at_event']
        price = row['close']

        if price >= z_high + (1.15 * atr):
            sl_price = z_low - (1.15 * atr)
            tp_price = price + (2.5 * atr)
            self.active_zone['triggered'] = True
            return self._build_multiplier_params("MULTUP", "BUY_GOLD_POSITION", price, sl_price, tp_price, session_status)

        elif price <= z_low - (1.15 * atr):
            sl_price = z_high + (1.15 * atr)
            tp_price = price - (2.5 * atr)
            self.active_zone['triggered'] = True
            return self._build_multiplier_params("MULTDOWN", "SELL_GOLD_POSITION", price, sl_price, tp_price, session_status)

        if row['high'] >= z_high + (1.0 * atr) and price < z_high + (1.0 * atr):
            sl_price = z_high + (1.5 * atr)
            tp_price = z_high
            self.active_zone['triggered'] = True
            return self._build_multiplier_params("MULTDOWN", "SELL_GOLD_REVERSION", price, sl_price, tp_price, session_status)

        elif row['low'] <= z_low - (1.0 * atr) and price > z_low - (1.0 * atr):
            sl_price = z_low - (1.5 * atr)
            tp_price = z_low
            self.active_zone['triggered'] = True
            return self._build_multiplier_params("MULTUP", "BUY_GOLD_REVERSION", price, sl_price, tp_price, session_status)

        return None

    def _build_multiplier_params(self, contract_type, signal_name, entry, sl_price, tp_price, session):
        sl_distance = abs(entry - sl_price)
        tp_distance = abs(tp_price - entry)

        sl_usd = round((sl_distance / entry) * self.stake * self.multiplier, 2)
        tp_usd = round((tp_distance / entry) * self.stake * self.multiplier, 2)

        sl_usd = min(sl_usd, round(self.stake * 0.95, 2))
        sl_usd = max(sl_usd, 1.0)
        tp_usd = max(tp_usd, 1.0)

        return {
            "contract_type": contract_type,
            "signal_name": signal_name,
            "session": session,
            "entry_estimate": round(entry, 2),
            "sl_usd": sl_usd,
            "tp_usd": tp_usd
        }


async def fetch_gold_candles(client, count=60):
    response = await client.send_request({
        "ticks_history": SYMBOL,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": CANDLE_GRANULARITY
    })
    if "error" in response:
        return None, response["error"]["message"]
    candles = response.get("candles", [])
    if not candles:
        return None, "No candle data returned (Market likely closed)."
    df = pd.DataFrame(candles)
    df['timestamp'] = pd.to_datetime(df['epoch'], unit='s', tz='UTC')
    if 'volume' not in df.columns:
        df['volume'] = (df['high'] - df['low']) * 100
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    return df, None


async def execute_and_monitor_trade(client, bot, order_params):
    log_print(f"\n>>> EXECUTING ORDER: {order_params['signal_name']} ({order_params['contract_type']}) <<<")
    
    buy_request = {
        "buy": 1,
        "price": bot.stake,
        "parameters": {
            "amount": bot.stake,
            "basis": "stake",
            "contract_type": order_params["contract_type"],
            "currency": "USD",
            "symbol": SYMBOL,
            "multiplier": bot.multiplier,
            "limit_order": {
                "stop_loss": order_params["sl_usd"],
                "take_profit": order_params["tp_usd"]
            }
        }
    }

    buy_response = await client.send_request(buy_request)
    if "error" in buy_response:
        log_print(f"[ORDER ERROR] {buy_response['error']['message']}")
        return

    contract_id = buy_response["buy"]["contract_id"]
    buy_price = buy_response["buy"]["buy_price"]
    log_print(f"[ORDER OPENED] Contract ID: {contract_id} | Stake: ${buy_price} | SL: -${order_params['sl_usd']} | TP: +${order_params['tp_usd']}")

    while True:
        status_resp = await client.send_request({"proposal_open_contract": 1, "contract_id": contract_id})
        poc = status_resp.get("proposal_open_contract", {})
        
        current_pnl = poc.get("profit", 0.0)
        is_sold = poc.get("is_sold", 0)
        entry_spot = poc.get("entry_tick", order_params["entry_estimate"])
        current_spot = poc.get("current_spot", entry_spot)

        log_print(f"   [LIVE P&L] Contract {contract_id} | Spot: {current_spot} | Floating P&L: ${current_pnl:+.2f}")

        if is_sold == 1:
            exit_spot = poc.get("exit_tick", current_spot)
            status_text = "WIN" if current_pnl > 0 else "LOSS"
            
            bal_resp = await client.send_request({"balance": 1})
            new_balance = bal_resp["balance"]["balance"]
            
            log_print(f"=== TRADE CLOSED [{status_text}] | Net P&L: ${current_pnl:+.2f} | New Balance: ${new_balance:.2f} ===\n")
            
            bot.log_trade_to_csv([
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                contract_id,
                order_params["signal_name"],
                order_params["session"],
                entry_spot,
                exit_spot,
                bot.stake,
                order_params["sl_usd"],
                order_params["tp_usd"],
                current_pnl,
                status_text,
                new_balance
            ])
            break

        await asyncio.sleep(5)


async def print_account_summary(client):
    pt = await client.send_request({"profit_table": 1, "limit": 10, "description": 1})
    transactions = pt.get("profit_table", {}).get("transactions", [])
    log_print("\n--- LAST 10 DERIV DEMO TRADES SUMMARY ---")
    total_pnl = 0.0
    for t in transactions:
        pnl = round(t["sell_price"] - t["buy_price"], 2)
        total_pnl += pnl
        log_print(f"ID: {t['contract_id']} | P&L: ${pnl:+.2f} | Payout: ${t['sell_price']}")
    log_print(f"Total Recent P&L: ${total_pnl:+.2f}\n")


async def run_live_bot():
    client = DirectDerivClient(DERIV_WS_URL)
    try:
        log_print("[1/3] Connecting to Deriv WebSocket...")
        await client.connect()
        
        log_print("[2/3] Authorizing Deriv Token...")
        auth = await client.send_request({"authorize": DERIV_API_TOKEN})
        if "error" in auth:
            log_print(f"[AUTH ERROR] {auth['error']['message']}")
            return

        acc_info = auth["authorize"]
        log_print(f"[SUCCESS] Connected to Deriv Account: {acc_info['loginid']} | Balance: {acc_info['balance']} {acc_info['currency']}")
        
        await print_account_summary(client)

        bot = ScottGoldDerivBot(stake=STAKE_AMOUNT, multiplier=MULTIPLIER)
        last_processed_epoch = None

        log_print("[3/3] Bot is running in LIVE MARKET mode on Gold (frxXAUUSD)... Press Ctrl+C to stop.\n")

        while True:
            df, err = await fetch_gold_candles(client, count=60)
            if err:
                log_print(f"[MARKET STATUS] {err} -> Waiting 60s before retry...")
                await asyncio.sleep(60)
                continue

            df = bot.calculate_indicators(df)
            latest_closed_candle = df.iloc[-2]
            candle_time = latest_closed_candle['timestamp']

            if last_processed_epoch != candle_time:
                last_processed_epoch = candle_time
                log_print(f"[{candle_time}] Gold Close: {latest_closed_candle['close']:.2f} | ATR: {latest_closed_candle['atr']:.2f} | RVOL: {latest_closed_candle['rvol']:.2f}x")
                
                bot.detect_volume_event(latest_closed_candle)
                signal = bot.check_signals(latest_closed_candle)

                if signal:
                    await execute_and_monitor_trade(client, bot, signal)
            else:
                log_print(f"[HEARTBEAT {datetime.now().strftime('%H:%M:%S')}] Waiting for next 5m candle close... (Last: {latest_closed_candle['close']:.2f})")

            await asyncio.sleep(15)

    except asyncio.TimeoutError:
        log_print("[TIMEOUT ERROR] Deriv server se response nahi aaya (Internet ya Firewall check karein).")
    except Exception as e:
        log_print(f"[ERROR] {type(e).__name__}: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_live_bot())
