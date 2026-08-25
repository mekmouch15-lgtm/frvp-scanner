"""
FRVP Quant Scalper - MEXC Multi-Symbol Scanner
Replicates the original TradingView Pine Script logic (HTF swing + Fixed Range
Volume Profile POC retest) directly against MEXC's public REST API, then sends
new BUY / TP / SL events to a Telegram channel via the Bot API.

Runs statelessly on each execution (e.g. every 15 minutes via GitHub Actions);
persists a small state.json (last processed candle time per symbol) so it
never re-sends historical signals, only genuinely new ones.
"""

import asyncio
import aiohttp
import json
import os
import sys
import time

MEXC_KLINES_URL = "https://api.mexc.com/api/v3/klines"
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ---- Strategy parameters (mirrors the original Pine Script inputs) --------
SWING_LEFT = 5
SWING_RIGHT = 5
PROFILE_ROWS = 50
MAX_PROFILE_BARS = 500          # cap on 15m candles used to build a profile
BODY_RATIO_MIN = 0.20
POC_TOUCH_PCT = 0.15

ENTRY2_PCT = 2.15
SL_PCT = 2.15
TP_PCTS = [2.20, 4.45, 6.75, 9.10, 11.51, 13.96]

KLINES_1H_LIMIT = 300            # ~12 days of 1H candles, for swing detection
KLINES_15M_LIMIT = 1000          # ~10 days of 15m candles, for profile + entries

STATE_FILE = "state.json"
SYMBOLS_FILE = "symbols.txt"

CONCURRENCY = 12                 # simultaneous requests to MEXC


# ============================================================================
# Data fetching
# ============================================================================

async def fetch_klines(session, sym, interval, limit, sem):
    url = f"{MEXC_KLINES_URL}?symbol={sym}&interval={interval}&limit={limit}"
    async with sem:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    await asyncio.sleep(1.0)
            except Exception:
                await asyncio.sleep(1.0)
    return None


def parse_klines(raw):
    if not raw:
        return []
    out = []
    for k in raw:
        try:
            out.append({
                "time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            })
        except (ValueError, IndexError):
            continue
    return out


# ============================================================================
# Swing pivot detection (equivalent of ta.pivotlow / ta.pivothigh)
# ============================================================================

def find_pivots(candles, left, right):
    lows, highs = [], []
    n = len(candles)
    for i in range(left, n - right):
        lo = candles[i]["low"]
        hi = candles[i]["high"]
        window = candles[i - left:i + right + 1]
        if all(lo <= c["low"] for c in window):
            lows.append((i, candles[i]["time"], lo))
        if all(hi >= c["high"] for c in window):
            highs.append((i, candles[i]["time"], hi))
    return lows, highs


# ============================================================================
# Fixed Range Volume Profile (POC only - Value Area not needed for retest)
# ============================================================================

def build_profile(candles_15m, start_time, end_time, rows, max_bars):
    window = [c for c in candles_15m if start_time <= c["time"] <= end_time]
    if len(window) > max_bars:
        window = window[-max_bars:]
    if len(window) < 5:
        return None
    profile_low = min(c["low"] for c in window)
    profile_high = max(c["high"] for c in window)
    if profile_high <= profile_low:
        return None
    step = (profile_high - profile_low) / rows
    if step <= 0:
        return None
    vols = [0.0] * rows
    for c in window:
        price = (c["high"] + c["low"]) / 2.0
        idx = int((price - profile_low) / step)
        idx = max(0, min(rows - 1, idx))
        vols[idx] += c["volume"]
    total = sum(vols)
    if total <= 0:
        return None
    poc_index = vols.index(max(vols))
    poc = profile_low + (poc_index + 0.5) * step
    return poc


# ============================================================================
# Core state machine - mirrors the Pine Script trade engine exactly
# ============================================================================

def run_engine(candles_1h, candles_15m):
    lows_1h, highs_1h = find_pivots(candles_1h, SWING_LEFT, SWING_RIGHT)
    lows_1h.sort(key=lambda x: x[1])
    highs_1h.sort(key=lambda x: x[1])

    last_swing_low = None
    last_swing_low_time = None
    li = hi = 0

    poc = None
    profile_ready = False
    book_armed = False
    in_trade = False
    entry_idx = -1
    entry1 = entry2 = stop_loss = None
    tps = [None] * 6
    tp_done = [False] * 6

    events = []  # (type, time, price)

    for idx, c in enumerate(candles_15m):
        t = c["time"]

        while li < len(lows_1h) and lows_1h[li][1] <= t:
            last_swing_low = lows_1h[li][2]
            last_swing_low_time = lows_1h[li][1]
            li += 1

        while hi < len(highs_1h) and highs_1h[hi][1] <= t:
            sh_time, sh_val = highs_1h[hi][1], highs_1h[hi][2]
            if (last_swing_low_time is not None and sh_time > last_swing_low_time
                    and sh_val > last_swing_low):
                p = build_profile(candles_15m, last_swing_low_time, sh_time,
                                   PROFILE_ROWS, MAX_PROFILE_BARS)
                if p is not None:
                    poc = p
                    profile_ready = True
            hi += 1

        if not (profile_ready and poc is not None):
            continue

        body_range = c["high"] - c["low"]
        body_ratio = abs(c["close"] - c["open"]) / body_range if body_range > 0 else 0.0
        touch_distance = poc * POC_TOUCH_PCT / 100.0

        if not in_trade:
            if c["close"] < poc:
                book_armed = False
            elif c["close"] > poc and not book_armed:
                book_armed = True

            retest = (book_armed and c["low"] <= poc + touch_distance
                      and c["close"] >= poc and c["close"] >= c["open"]
                      and body_ratio >= BODY_RATIO_MIN)

            if retest:
                entry1 = poc
                entry2 = entry1 * (1 - ENTRY2_PCT / 100)
                stop_loss = entry2 * (1 - SL_PCT / 100)
                tps = [entry1 * (1 + p / 100) for p in TP_PCTS]
                tp_done = [False] * 6
                in_trade = True
                entry_idx = idx
                book_armed = False
                events.append(("BUY", t, {
                    "entry1": entry1, "entry2": entry2, "sl": stop_loss,
                    "tp": tps,
                }))
        else:
            if idx > entry_idx:
                if c["low"] <= stop_loss:
                    events.append(("SL", t, {"price": stop_loss}))
                    in_trade = False
                else:
                    for i in range(6):
                        if not tp_done[i] and c["high"] >= tps[i]:
                            tp_done[i] = True
                            events.append((f"TP{i + 1}", t, {"price": tps[i]}))
                    if tp_done[5]:
                        in_trade = False

    return events


# ============================================================================
# Telegram
# ============================================================================

async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env vars", file=sys.stderr)
        return
    url = TELEGRAM_API.format(token=TELEGRAM_TOKEN)
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"Telegram error {resp.status}: {body}", file=sys.stderr)
    except Exception as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)


def format_event(symbol, ev_type, data):
    if ev_type == "BUY":
        tp = data["tp"]
        return (
            f"🟢 <b>{symbol}</b> — BUY (POC Retest)\n"
            f"Entry 1: {data['entry1']:.6g}\n"
            f"Entry 2: {data['entry2']:.6g}\n"
            f"SL: {data['sl']:.6g}\n"
            f"TP1: {tp[0]:.6g}  TP2: {tp[1]:.6g}  TP3: {tp[2]:.6g}\n"
            f"TP4: {tp[3]:.6g}  TP5: {tp[4]:.6g}  TP6: {tp[5]:.6g}"
        )
    if ev_type == "SL":
        return f"🔴 <b>{symbol}</b> — Stop Loss hit at {data['price']:.6g}"
    return f"🟡 <b>{symbol}</b> — {ev_type} hit at {data['price']:.6g}"


# ============================================================================
# Per-symbol processing
# ============================================================================

async def process_symbol(session, sem, symbol, state):
    api_symbol = symbol.replace("MEXC:", "").strip()
    if not api_symbol:
        return None

    raw_1h, raw_15m = await asyncio.gather(
        fetch_klines(session, api_symbol, "60m", KLINES_1H_LIMIT, sem),
        fetch_klines(session, api_symbol, "15m", KLINES_15M_LIMIT, sem),
    )
    candles_1h = parse_klines(raw_1h)
    candles_15m = parse_klines(raw_15m)

    if len(candles_1h) < (SWING_LEFT + SWING_RIGHT + 5) or len(candles_15m) < 20:
        return None  # not enough data / symbol not on MEXC as expected

    events = run_engine(candles_1h, candles_15m)
    last_candle_time = candles_15m[-1]["time"]

    prev_time = state.get(api_symbol, 0)
    new_events = [e for e in events if e[1] > prev_time] if prev_time else []

    state[api_symbol] = last_candle_time
    return api_symbol, new_events


# ============================================================================
# Main
# ============================================================================

async def main():
    if not os.path.exists(SYMBOLS_FILE):
        print(f"Missing {SYMBOLS_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(SYMBOLS_FILE) as f:
        symbols = [line.strip() for line in f if line.strip()]

    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            try:
                state = json.load(f)
            except json.JSONDecodeError:
                state = {}

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [process_symbol(session, sem, s, state) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        alerts_to_send = []
        for r in results:
            if isinstance(r, Exception) or r is None:
                continue
            api_symbol, new_events = r
            for ev_type, t, data in new_events:
                alerts_to_send.append((api_symbol, ev_type, t, data))

        alerts_to_send.sort(key=lambda x: x[2])

        print(f"Processed {len(symbols)} symbols, {len(alerts_to_send)} new alerts")

        for api_symbol, ev_type, t, data in alerts_to_send:
            text = format_event(api_symbol, ev_type, data)
            await send_telegram(session, text)
            await asyncio.sleep(0.3)  # gentle on Telegram's rate limit

    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


if __name__ == "__main__":
    asyncio.run(main())
