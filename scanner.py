"""
FRVP Quant Scalper - MEXC Multi-Symbol Scanner
Replicates the original TradingView Pine Script logic (HTF swing + Fixed Range
Volume Profile POC retest) directly against MEXC's public REST API, then sends
new BUY / TP / SL events to a Telegram channel via the Bot API.

TP/SL messages are sent as Telegram replies to their original BUY signal
message, so tapping them jumps back to the original trade card.

Runs statelessly on each execution (e.g. every 15 minutes via GitHub Actions);
persists state.json (last processed candle + open trade per symbol) so it
never re-sends historical signals, only genuinely new ones.
"""

import asyncio
import aiohttp
import json
import os
import sys

MEXC_KLINES_URL = "https://api.mexc.com/api/v3/klines"
TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"

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

CONCURRENCY = 25                 # simultaneous requests to MEXC (raised so a
                                  # full 564-symbol run comfortably finishes
                                  # well under the 1-minute schedule interval)


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

    events = []  # (type, time, data)

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

async def send_telegram(session, text, reply_to=None):
    """Sends a message and returns its message_id (or None on failure)."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID env vars", file=sys.stderr)
        return None
    url = TELEGRAM_API.format(token=TELEGRAM_TOKEN, method="sendMessage")
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            body = await resp.json()
            if resp.status == 200 and body.get("ok"):
                return body["result"]["message_id"]
            print(f"Telegram error {resp.status}: {body}", file=sys.stderr)
    except Exception as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)
    return None


def pct_vs_entry(price, entry1):
    return (price - entry1) / entry1 * 100.0


def fmt_price(x):
    return f"{x:.6g}"


def fmt_pair(symbol):
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    return f"${base} | #{symbol}"


def fmt_duration(ms):
    total_seconds = max(0, int(ms / 1000))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def format_buy_message(symbol, data):
    e1, e2, sl, tp = data["entry1"], data["entry2"], data["sl"], data["tp"]
    lines = [
        "📈 <b>NEW TRADING SIGNAL</b> 📈",
        "",
        f"📊 Pair: {fmt_pair(symbol)}",
        "",
        "🎯 <b>ENTRIES:</b>",
        f"🟢 Entry 1: {fmt_price(e1)}",
        f"🟢 Entry 2: {fmt_price(e2)} [{pct_vs_entry(e2, e1):+.2f}%]",
        "",
        f"⛔ Stop Loss: {fmt_price(sl)} [{pct_vs_entry(sl, e1):+.2f}%]",
        "",
        "💎 <b>TARGETS:</b>",
    ]
    for i, tp_price in enumerate(tp, start=1):
        lines.append(f"✅ TP{i}: {fmt_price(tp_price)} [{pct_vs_entry(tp_price, e1):+.2f}%]")
    return "\n".join(lines)


def format_hit_message(symbol, ev_type, price, entry1, entry_time_ms, hit_time_ms, trade_id):
    gain = pct_vs_entry(price, entry1)
    speed = fmt_duration(hit_time_ms - entry_time_ms)
    is_sl = (ev_type == "SL")
    header = "🛑 <b>STOP LOSS HIT</b> 🛑" if is_sl else f"✅🎉 <b>TARGET {ev_type} HIT</b> 🎉✅"
    gain_icon = "📉" if gain < 0 else "📈"
    lines = [
        header,
        f"💎 {symbol}",
        f"{gain_icon} Gain: {gain:+.2f}%",
        f"⏱ Speed: {speed}",
        f"🆔 ID: {trade_id}",
    ]
    return "\n".join(lines)


# ============================================================================
# Per-symbol processing (data fetch + engine only, no sending here)
# ============================================================================

async def fetch_and_run(session, sem, symbol):
    api_symbol = symbol.replace("MEXC:", "").strip()
    if not api_symbol:
        return None

    raw_1h, raw_15m = await asyncio.gather(
        fetch_klines(session, api_symbol, "60m", KLINES_1H_LIMIT, sem),
        fetch_klines(session, api_symbol, "15m", KLINES_15M_LIMIT, sem),
    )
    candles_1h = parse_klines(raw_1h)
    candles_15m = parse_klines(raw_15m)

    # MEXC's klines endpoint returns the still-forming (unclosed) candle as
    # the last row. Using it would let a trade's entry and its TP/SL checks
    # both fall inside that same wide, incomplete candle - producing
    # impossible-looking "instant" TP cascades. Drop it, mirroring the
    # original Pine script's barstate.isconfirmed guard (closed bars only).
    candles_1h = candles_1h[:-1] if len(candles_1h) > 1 else candles_1h
    candles_15m = candles_15m[:-1] if len(candles_15m) > 1 else candles_15m

    if len(candles_1h) < (SWING_LEFT + SWING_RIGHT + 5) or len(candles_15m) < 20:
        return None

    events = run_engine(candles_1h, candles_15m)
    last_candle_time = candles_15m[-1]["time"]
    return api_symbol, events, last_candle_time


# ============================================================================
# State helpers
# ============================================================================

def normalize_symbol_state(raw):
    """Old state format was symbol -> int(last_time). Upgrade to dict form."""
    if isinstance(raw, dict):
        return raw
    return {"last_time": raw or 0, "open": None}


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

    counter = state.get("_counter", 10000)

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)

    total_alerts = 0

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_run(session, sem, s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception) or r is None:
                continue
            api_symbol, events, last_candle_time = r

            sym_state = normalize_symbol_state(state.get(api_symbol))
            prev_time = sym_state.get("last_time", 0)
            open_trade = sym_state.get("open")

            new_events = [e for e in events if e[1] > prev_time] if prev_time else []

            for ev_type, t, data in new_events:
                if ev_type == "BUY":
                    text = format_buy_message(api_symbol, data)
                    msg_id = await send_telegram(session, text)
                    counter += 1
                    open_trade = {
                        "message_id": msg_id,
                        "entry_time": t,
                        "entry1": data["entry1"],
                        "trade_id": counter,
                    }
                    total_alerts += 1
                else:
                    entry1 = open_trade["entry1"] if open_trade else data["price"]
                    entry_time = open_trade["entry_time"] if open_trade else t
                    trade_id = open_trade["trade_id"] if open_trade else counter
                    reply_to = open_trade["message_id"] if open_trade else None

                    text = format_hit_message(api_symbol, ev_type, data["price"],
                                               entry1, entry_time, t, trade_id)
                    await send_telegram(session, text, reply_to=reply_to)
                    total_alerts += 1

                    if ev_type in ("SL", "TP6"):
                        open_trade = None

                await asyncio.sleep(0.3)  # gentle on Telegram's rate limit

            state[api_symbol] = {"last_time": last_candle_time, "open": open_trade}

        print(f"Processed {len(symbols)} symbols, {total_alerts} new alerts")

    state["_counter"] = counter
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


if __name__ == "__main__":
    asyncio.run(main())
