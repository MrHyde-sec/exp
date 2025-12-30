#!/usr/bin/env python3
"""
Bybit WebSocket Sweep Scalper (5m structure + ATR buffer)

Strategy (A):
- Maintain a rolling 5-minute range (highest high / lowest low over RANGE_LOOKBACK candles).
- Detect a "liquidity sweep" when last traded price pushes beyond a range boundary by BUFFER = ATR_MULT * ATR.
- Require the sweep to persist for at least MIN_SWEEP_DURATION_SEC (and at least MIN_SWEEP_PRINTS prints) to filter one-tick noise.
- Enter on "reclaim": price returns back inside the range boundary and holds for RECLAIM_HOLD_SEC.
- Take profit: range midpoint.
- Stop loss: beyond the sweep extreme by SL_ATR_MULT * ATR.
- Daily max trades: 5 (configurable).

Data:
- Uses Bybit V5 public WebSocket streams (ticker + trade + kline) via `pybit`.
Trading:
- Uses Bybit V5 HTTP endpoints via `pybit` for orders/positions.

⚠️ Risks:
- This is a demo-quality bot. Use testnet/demo first.
- If the process dies, exchange-side TP/SL is *attempted* via set_trading_stop, but if that fails it falls back to internal exits.

Requirements:
  pip install pybit python-dotenv

.env variables (recommended):
  BYBIT_API_KEY=...
  BYBIT_API_SECRET=...
  BYBIT_TESTNET=true   # or false
"""

from __future__ import annotations

import os
import json
import math
import time
import signal
import logging
import sys
import threading
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, Optional, List, Tuple

from dotenv import load_dotenv

# pybit (official Bybit SDK)
from pybit.unified_trading import HTTP, WebSocket  # type: ignore


# =============================
# CONFIG (tune these)
# =============================

SYMBOLS: List[str] = [
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",  # chosen over ZEC for liquidity
]

CATEGORY = "linear"  # USDT perpetuals
KLINE_INTERVAL_MIN = 5  # 5m structure

# Structure / indicators
RANGE_LOOKBACK = 24          # 24 * 5m = 2 hours
ATR_PERIOD = 14
ATR_MULT_BUFFER = 0.35       # your request
SL_ATR_MULT = 0.55           # stop beyond sweep extreme
MIN_RANGE_ATR_MULT = 0.8     # avoid ultra-tight chop

# Sweep / reclaim micro-structure
MIN_SWEEP_DURATION_SEC = 0.35   # ✅ "minimum sweep duration" (best balance: filters 1-tick noise but keeps trade count high)
MIN_SWEEP_PRINTS = 2            # require at least 2 trade updates beyond threshold
MAX_SWEEP_AGE_SEC = 180         # must reclaim within this, else abandon sweep
RECLAIM_HOLD_SEC = 0.25         # how long price must stay back inside boundary to confirm reclaim
COOLDOWN_AFTER_SIGNAL_SEC = 60  # per-symbol cooldown after any entry/attempt

# Execution / risk
MAX_TRADES_PER_DAY = 5
MAX_CONCURRENT_POSITIONS = 1    # keep it simple for small margin
LEVERAGE_DEFAULT = 3            # "you decide" -> conservative starter; change later after demo survival

RISK_PER_TRADE_USDT = 1.0       # target *loss* per trade (roughly)
MAX_NOTIONAL_PER_TRADE_USDT = 25.0  # cap size even if stop is tiny
MIN_NOTIONAL_PER_TRADE_USDT = 5.0   # avoid too-small orders that fail min qty

MAX_SPREAD_BPS = 8.0            # don't enter if spread too wide (bps = 0.01%)

# Dashboard
DASHBOARD_REFRESH_SEC = 5.0  # refresh terminal dashboard every N seconds
MAX_SLIPPAGE_BPS = 15.0         # if execution price deviates too far, skip (best-effort)

# Safety
ENABLE_DAILY_LOSS_LIMIT = False
DAILY_LOSS_LIMIT_USDT = 10.0    # if enabled, stop trading after losing this much in a UTC day

# Persistence
STATE_DIR = "./state"
DAILY_STATE_FILE = os.path.join(STATE_DIR, "bybit_scalper_daily_state.json")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# =============================
# LOGGING
# =============================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bybit_ws_scalper")


# =============================
# HELPERS
# =============================

def utc_date_str() -> str:
    """UTC date (NOT datetime) so daily logic doesn't reset every loop."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def now_ms() -> int:
    return int(time.time() * 1000)

def safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def round_to_tick(x: float, tick: float) -> float:
    if tick <= 0:
        return x
    # round to nearest tick, then re-round to avoid float imprecision
    r = round(x / tick) * tick
    return float(f"{r:.12f}")

def spread_bps(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0:
        return 1e9
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 1e9
    return (ask - bid) / mid * 10000.0



def _fmt_age(seconds: float) -> str:
    if seconds < 0:
        seconds = 0
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"

def fetch_equity_usdt(http) -> float:
    """
    Get account equity (USDT). Works with UTA (UNIFIED) and falls back to CONTRACT.
    Returns 0.0 if unavailable.
    """
    def _try(account_type: str) -> float:
        r = http.get_wallet_balance(accountType=account_type, coin="USDT")
        if not r or r.get("retCode") != 0:
            return 0.0

        # Expected: result.list[0].totalEquity or totalWalletBalance
        lst = r.get("result", {}).get("list", [])
        if not lst:
            return 0.0

        info = lst[0]
        equity = info.get("totalEquity") or info.get("totalWalletBalance")
        return float(equity) if equity is not None else 0.0

    eq = _try("UNIFIED")
    if eq > 0:
        return eq
    eq = _try("CONTRACT")
    return eq

# =============================
# DATA STRUCTURES
# =============================

@dataclass
class Candle:
    start_ms: int
    end_ms: int
    o: float
    h: float
    l: float
    c: float

@dataclass
class InstrumentRules:
    qty_step: float = 0.001
    min_qty: float = 0.001
    tick_size: float = 0.01
    min_notional: float = 0.0

@dataclass
class SweepState:
    active: bool = False
    direction: Optional[str] = None  # "up" or "down"
    start_ts: float = 0.0
    last_beyond_ts: float = 0.0
    extreme: float = 0.0
    prints: int = 0
    valid: bool = False

    reclaim_started_ts: Optional[float] = None

    def reset(self):
        self.active = False
        self.direction = None
        self.start_ts = 0.0
        self.last_beyond_ts = 0.0
        self.extreme = 0.0
        self.prints = 0
        self.valid = False
        self.reclaim_started_ts = None

@dataclass
class SymbolState:
    symbol: str
    lock: threading.Lock = field(default_factory=threading.Lock)

    # market data
    bid: float = 0.0
    ask: float = 0.0
    last_price: float = 0.0
    last_trade_ts: float = 0.0

    # latency (trade stream): local_now - exchange_trade_ts (seconds)
    last_trade_latency_s: float = 0.0
    latency_ema_s: float = 0.0

    # candles (closed only)
    candles: Deque[Candle] = field(default_factory=lambda: deque(maxlen=400))

    # derived
    atr: float = 0.0
    range_high: float = 0.0
    range_low: float = 0.0
    mid: float = 0.0
    last_struct_update_ms: int = 0

    # micro-structure
    sweep: SweepState = field(default_factory=SweepState)
    cooldown_until: float = 0.0

@dataclass
class PositionState:
    symbol: str = ""
    side: str = ""              # "Buy" or "Sell"
    qty: float = 0.0
    entry_price: float = 0.0
    open_ts: float = 0.0
    tp: float = 0.0
    sl: float = 0.0
    active: bool = False


# =============================
# DAILY STATE (persist)
# =============================

DAILY_DATE_UTC: str = ""
DAILY_START_EQUITY: Optional[float] = None
DAILY_TRADES_TODAY: int = 0
KILL_SWITCH: bool = False
KILL_REASON: str = ""

def load_daily_state():
    global DAILY_DATE_UTC, DAILY_START_EQUITY, DAILY_TRADES_TODAY
    os.makedirs(STATE_DIR, exist_ok=True)
    if not os.path.exists(DAILY_STATE_FILE):
        return
    try:
        with open(DAILY_STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
        DAILY_DATE_UTC = st.get("date", "")
        DAILY_START_EQUITY = st.get("start_equity", None)
        if DAILY_START_EQUITY is not None:
            DAILY_START_EQUITY = safe_float(DAILY_START_EQUITY, default=None)  # type: ignore
        DAILY_TRADES_TODAY = int(st.get("trades_today", 0))
    except Exception as e:
        log.warning(f"[WARN] daily state load failed: {e}")

def save_daily_state():
    os.makedirs(STATE_DIR, exist_ok=True)
    st = {
        "date": DAILY_DATE_UTC,
        "start_equity": DAILY_START_EQUITY,
        "trades_today": DAILY_TRADES_TODAY,
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        with open(DAILY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, indent=2)
    except Exception as e:
        log.warning(f"[WARN] daily state save failed: {e}")

def init_or_roll_daily_state(current_equity: float):
    global DAILY_DATE_UTC, DAILY_START_EQUITY, DAILY_TRADES_TODAY, KILL_SWITCH, KILL_REASON
    today = utc_date_str()
    # If equity fetch failed (e.g., timestamp/recv_window error), do NOT reset baseline to 0.
    if current_equity <= 0 and DAILY_START_EQUITY is not None and DAILY_DATE_UTC == today:
        return
    if current_equity <= 0 and (DAILY_START_EQUITY is None or DAILY_DATE_UTC != today):
        log.warning(f"[WARN] Equity unavailable (={current_equity}). Skipping daily reset until equity is fetched.")
        return
    if DAILY_DATE_UTC == today and DAILY_START_EQUITY is not None:
        return
    DAILY_DATE_UTC = today
    DAILY_START_EQUITY = float(current_equity)
    DAILY_TRADES_TODAY = 0
    KILL_SWITCH = False
    KILL_REASON = ""
    save_daily_state()
    log.info(f"[OK] Daily reset: date={DAILY_DATE_UTC} start_equity={DAILY_START_EQUITY:.2f}")

def daily_pnl(current_equity: float) -> float:
    if DAILY_START_EQUITY is None:
        return 0.0
    return current_equity - float(DAILY_START_EQUITY)

def update_kill_switch(current_equity: float):
    global KILL_SWITCH, KILL_REASON
    init_or_roll_daily_state(current_equity)
    if not ENABLE_DAILY_LOSS_LIMIT:
        return
    pnl = daily_pnl(current_equity)
    if pnl <= -abs(float(DAILY_LOSS_LIMIT_USDT)):
        KILL_SWITCH = True
        KILL_REASON = f"daily_loss pnl={pnl:.2f} <= -{abs(float(DAILY_LOSS_LIMIT_USDT)):.2f}"


# =============================
# INDICATORS
# =============================

def compute_atr(candles: List[Candle], period: int) -> float:
    if len(candles) < period + 1:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(candles)):
        c_prev = candles[i - 1].c
        h = candles[i].h
        l = candles[i].l
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(tr)
    if len(trs) < period:
        return 0.0
    # simple average
    window = trs[-period:]
    return sum(window) / float(period)

def compute_range(candles: List[Candle], lookback: int) -> Tuple[float, float]:
    if len(candles) < lookback:
        return 0.0, 0.0
    window = candles[-lookback:]
    hi = max(c.h for c in window)
    lo = min(c.l for c in window)
    return hi, lo


# =============================
# BYBIT CLIENT
# =============================

class BybitClient:
    def __init__(self, api_key: str, api_secret: str, testnet: bool):
        # HTTP client
        self.http = HTTP(
            testnet=testnet,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=20000,  # tolerate clock drift/latency (ErrCode 10002)
        )
        self.testnet = testnet

    def get_equity_usdt(self) -> float:
        """
        Best-effort equity for daily PnL.
        Bybit responses vary by account mode; we parse robustly.
        """
        # try UNIFIED
        for account_type in ("UNIFIED", "CONTRACT"):
            try:
                res = self.http.get_wallet_balance(accountType=account_type)
                if not isinstance(res, dict):
                    continue
                r = res.get("result", {}) or {}
                lst = r.get("list", []) or []
                if not lst:
                    continue
                # often list[0] contains totals
                obj0 = lst[0] if isinstance(lst[0], dict) else {}
                # common fields
                for k in ("totalEquity", "equity", "totalWalletBalance", "walletBalance"):
                    if k in obj0 and obj0[k] not in (None, "", "0"):
                        return safe_float(obj0[k], default=0.0)
                # sometimes per-coin list inside "coin"
                coins = obj0.get("coin", []) or []
                if isinstance(coins, list):
                    for c in coins:
                        if isinstance(c, dict) and c.get("coin") == "USDT":
                            for k in ("equity", "walletBalance", "availableToWithdraw", "availableBalance"):
                                if k in c and c[k] not in (None, "", "0"):
                                    return safe_float(c[k], default=0.0)
            except Exception:
                continue
        return 0.0

    def get_instrument_rules(self, symbol: str) -> InstrumentRules:
        try:
            res = self.http.get_instruments_info(category=CATEGORY, symbol=symbol)
            r = res.get("result", {}) or {}
            lst = r.get("list", []) or []
            if not lst:
                return InstrumentRules()
            info = lst[0]
            price_filter = info.get("priceFilter", {}) or {}
            lot_filter = info.get("lotSizeFilter", {}) or {}
            tick = safe_float(price_filter.get("tickSize"), default=0.01)
            qty_step = safe_float(lot_filter.get("qtyStep"), default=0.001)
            min_qty = safe_float(lot_filter.get("minOrderQty"), default=qty_step)
            return InstrumentRules(qty_step=qty_step, min_qty=min_qty, tick_size=tick)
        except Exception as e:
            log.warning(f"[WARN] get_instrument_rules failed for {symbol}: {e}")
            return InstrumentRules()

    def set_leverage(self, symbol: str, leverage: int) -> None:
        try:
            # buyLeverage/sellLeverage as strings in Bybit API
            self.http.set_leverage(
                category=CATEGORY,
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage),
            )
            log.info(f"[OK] Set leverage {leverage}x for {symbol}")
        except Exception as e:
            log.warning(f"[WARN] set_leverage failed for {symbol}: {e}")

    def get_kline_history(self, symbol: str, limit: int = 200) -> List[Candle]:
        """
        Fetch historical klines via REST to bootstrap indicator.
        """
        try:
            res = self.http.get_kline(
                category=CATEGORY,
                symbol=symbol,
                interval=str(KLINE_INTERVAL_MIN),
                limit=limit,
            )
            r = res.get("result", {}) or {}
            lst = r.get("list", []) or []
            candles: List[Candle] = []
            for item in lst:
                # Bybit commonly returns list like [start, open, high, low, close, volume, turnover]
                if isinstance(item, list) and len(item) >= 5:
                    start = int(item[0])
                    o = safe_float(item[1])
                    h = safe_float(item[2])
                    l = safe_float(item[3])
                    c = safe_float(item[4])
                    end = start + KLINE_INTERVAL_MIN * 60_000 - 1
                    candles.append(Candle(start_ms=start, end_ms=end, o=o, h=h, l=l, c=c))
                elif isinstance(item, dict):
                    start = int(item.get("start", 0))
                    end = int(item.get("end", start + KLINE_INTERVAL_MIN * 60_000 - 1))
                    candles.append(Candle(
                        start_ms=start,
                        end_ms=end,
                        o=safe_float(item.get("open")),
                        h=safe_float(item.get("high")),
                        l=safe_float(item.get("low")),
                        c=safe_float(item.get("close")),
                    ))
            # Bybit often returns newest first; we want oldest->newest
            candles.sort(key=lambda x: x.start_ms)
            return candles
        except Exception as e:
            log.warning(f"[WARN] get_kline_history failed for {symbol}: {e}")
            return []

    def place_market_order(self, symbol: str, side: str, qty: float, reduce_only: bool = False) -> Optional[str]:
        try:
            res = self.http.place_order(
                category=CATEGORY,
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=str(qty),
                timeInForce="IOC",
                reduceOnly=reduce_only,
                positionIdx=0,  # one-way mode
            )
            oid = None
            if isinstance(res, dict):
                oid = (res.get("result", {}) or {}).get("orderId")
            log.info(f"[OK] place_order {symbol} {side} qty={qty} reduceOnly={reduce_only} orderId={oid}")
            return oid
        except Exception as e:
            log.error(f"[ERR] place_market_order failed {symbol} {side} qty={qty}: {e}")
            return None

    def set_trading_stop(self, symbol: str, tp: float, sl: float) -> bool:
        """
        Exchange-side protective stops (preferred). If this fails, bot will still try internal exits.
        """
        try:
            self.http.set_trading_stop(
                category=CATEGORY,
                symbol=symbol,
                takeProfit=str(tp),
                stopLoss=str(sl),
                tpTriggerBy="LastPrice",
                slTriggerBy="LastPrice",
                positionIdx=0,
            )
            log.info(f"[OK] set_trading_stop {symbol} tp={tp:.6f} sl={sl:.6f}")
            return True
        except Exception as e:
            log.warning(f"[WARN] set_trading_stop failed for {symbol}: {e}")
            return False

    def get_position_qty_and_entry(self, symbol: str) -> Tuple[float, float, str]:
        """
        Returns (qty, entry_price, side) where side is "Buy" for long, "Sell" for short, "" for flat.
        """
        try:
            res = self.http.get_positions(category=CATEGORY, symbol=symbol)
            r = res.get("result", {}) or {}
            lst = r.get("list", []) or []
            if not lst:
                return 0.0, 0.0, ""
            pos = lst[0]
            size = safe_float(pos.get("size"), default=0.0)
            side = pos.get("side", "") or ""
            entry = safe_float(pos.get("avgPrice"), default=0.0)
            if size <= 0:
                return 0.0, 0.0, ""
            return size, entry, side
        except Exception as e:
            log.warning(f"[WARN] get_positions failed for {symbol}: {e}")
            return 0.0, 0.0, ""


# =============================
# STRATEGY ENGINE
# =============================

class SweepScalper:
    def __init__(self, client: BybitClient, symbols: List[str]):
        self.client = client
        self.symbols = symbols

        self.rules: Dict[str, InstrumentRules] = {s: InstrumentRules() for s in symbols}
        self.state: Dict[str, SymbolState] = {s: SymbolState(symbol=s) for s in symbols}

        self.pos_lock = threading.Lock()
        self.position = PositionState()
        self.running = True

        self.shutdown = False

        # websocket handles
        self.ws_public: Optional[WebSocket] = None

    # ---------- WebSocket callbacks ----------

    def _on_ticker(self, message: dict):
        try:
            data = message.get("data", None)
            if data is None:
                return
            # docs show snapshot returns dict for linear; sometimes list
            if isinstance(data, list) and data:
                items = data
            elif isinstance(data, dict):
                items = [data]
            else:
                return

            for it in items:
                sym = it.get("symbol") or it.get("s")
                if sym not in self.state:
                    continue
                bid = safe_float(it.get("bid1Price") or it.get("bidPrice"), default=0.0)
                ask = safe_float(it.get("ask1Price") or it.get("askPrice"), default=0.0)
                last = safe_float(it.get("lastPrice"), default=0.0)
                st = self.state[sym]
                with st.lock:
                    if bid > 0:
                        st.bid = bid
                    if ask > 0:
                        st.ask = ask
                    if last > 0:
                        st.last_price = last
        except Exception as e:
            log.debug(f"ticker cb error: {e}")

    def _on_trade(self, message: dict):
        try:
            data = message.get("data", None)
            if not isinstance(data, list) or not data:
                return

            # Per docs: up to 1024 trades per message; process the last trade as "latest" price/ts.
            last_trade = data[-1]
            sym = last_trade.get("s") or last_trade.get("symbol")
            if sym not in self.state:
                return
            price = safe_float(last_trade.get("p") or last_trade.get("price"), default=0.0)
            ts = safe_float(last_trade.get("T") or last_trade.get("ts") or message.get("ts"), default=time.time() * 1000.0)
            st = self.state[sym]
            with st.lock:
                if price > 0:
                    st.last_price = price
                exch_ts = float(ts) / 1000.0
                st.last_trade_ts = exch_ts
                local_now = time.time()
                lat = local_now - exch_ts
                if -5.0 < lat < 5.0:
                    st.last_trade_latency_s = lat
                    # EMA (fast-ish): alpha=0.2
                    if st.latency_ema_s == 0.0:
                        st.latency_ema_s = lat
                    else:
                        st.latency_ema_s = 0.2 * lat + 0.8 * st.latency_ema_s
        except Exception as e:
            log.debug(f"trade cb error: {e}")

    def _on_kline(self, message: dict):
        try:
            data = message.get("data", None)
            if not isinstance(data, list) or not data:
                return
            it = data[0]
            sym = it.get("symbol") or message.get("topic", "").split(".")[-1]
            if sym not in self.state:
                return

            confirm = bool(it.get("confirm", False))
            if not confirm:
                return  # only closed candles

            start = int(it.get("start", 0))
            end = int(it.get("end", start + KLINE_INTERVAL_MIN * 60_000 - 1))
            o = safe_float(it.get("open"), 0.0)
            h = safe_float(it.get("high"), 0.0)
            l = safe_float(it.get("low"), 0.0)
            c = safe_float(it.get("close"), 0.0)

            if start <= 0 or h <= 0 or l <= 0 or c <= 0:
                return

            st = self.state[sym]
            with st.lock:
                st.candles.append(Candle(start_ms=start, end_ms=end, o=o, h=h, l=l, c=c))
                # recompute indicators
                candles = list(st.candles)
                st.atr = compute_atr(candles, ATR_PERIOD)
                hi, lo = compute_range(candles, RANGE_LOOKBACK)
                st.range_high, st.range_low = hi, lo
                st.mid = (hi + lo) / 2.0 if hi > 0 and lo > 0 else 0.0
                st.last_struct_update_ms = now_ms()
        except Exception as e:
            log.debug(f"kline cb error: {e}")
            
    def stop(self):
        """Gracefully stop the bot (called by signal handler)."""
        self.running = False
        self.shutdown = True
        ws = getattr(self, "ws_public", None)
        if ws:
            try:
                ws.exit()
            except Exception:
                pass

    # ---------- Bootstrap ----------

    def bootstrap(self):
        log.info("[BOOT] Loading daily state...")
        load_daily_state()

        log.info("[BOOT] Loading instrument rules + leverage + initial candles...")
        for s in self.symbols:
            self.rules[s] = self.client.get_instrument_rules(s)
            self.client.set_leverage(s, LEVERAGE_DEFAULT)

            hist = self.client.get_kline_history(s, limit=200)
            st = self.state[s]
            with st.lock:
                for c in hist:
                    st.candles.append(c)
                candles = list(st.candles)
                st.atr = compute_atr(candles, ATR_PERIOD)
                hi, lo = compute_range(candles, RANGE_LOOKBACK)
                st.range_high, st.range_low = hi, lo
                st.mid = (hi + lo) / 2.0 if hi > 0 and lo > 0 else 0.0
                st.last_struct_update_ms = now_ms()

            log.info(
                f"[BOOT] {s} rules qty_step={self.rules[s].qty_step} min_qty={self.rules[s].min_qty} tick={self.rules[s].tick_size} "
                f"atr={st.atr:.6f} range=[{st.range_low:.6f},{st.range_high:.6f}]"
            )

        eq = self.client.get_equity_usdt()
        init_or_roll_daily_state(eq)

    # ---------- Risk / sizing ----------

    def can_trade_now(self) -> bool:
        if KILL_SWITCH:
            return False
        if DAILY_TRADES_TODAY >= MAX_TRADES_PER_DAY:
            return False
        return True

    def _has_open_position(self) -> bool:
        with self.pos_lock:
            return self.position.active

    def _open_positions_count(self) -> int:
        with self.pos_lock:
            return 1 if self.position.active else 0

    def _set_position(self, pos: PositionState):
        with self.pos_lock:
            self.position = pos

    def _clear_position(self):
        with self.pos_lock:
            self.position = PositionState()

    def _calc_order_qty(self, symbol: str, entry: float, stop: float) -> float:
        rules = self.rules[symbol]
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            return 0.0

        qty = float(RISK_PER_TRADE_USDT) / risk_per_unit

        # cap notional
        notional = qty * entry
        if notional > MAX_NOTIONAL_PER_TRADE_USDT:
            qty = MAX_NOTIONAL_PER_TRADE_USDT / entry

        # enforce minimum notional (so exchange min qty doesn't reject)
        if qty * entry < MIN_NOTIONAL_PER_TRADE_USDT:
            qty = MIN_NOTIONAL_PER_TRADE_USDT / entry

        # round down to step
        qty = floor_to_step(qty, rules.qty_step)

        # ensure >= min qty
        if qty < rules.min_qty:
            qty = 0.0
        return qty

    # ---------- Strategy evaluation ----------

    def _update_sweep_state(self, st: SymbolState, price: float, t: float):
        """
        Updates st.sweep based on price relative to range boundaries.
        """
        if st.atr <= 0 or st.range_high <= 0 or st.range_low <= 0:
            return

        buffer = ATR_MULT_BUFFER * st.atr

        # abandon old sweep
        if st.sweep.active and (t - st.sweep.start_ts) > MAX_SWEEP_AGE_SEC:
            st.sweep.reset()

        # determine beyond conditions
        beyond_up = price > (st.range_high + buffer)
        beyond_down = price < (st.range_low - buffer)

        if not st.sweep.active:
            if beyond_up:
                st.sweep.active = True
                st.sweep.direction = "up"
                st.sweep.start_ts = t
                st.sweep.last_beyond_ts = t
                st.sweep.extreme = price
                st.sweep.prints = 1
                st.sweep.valid = False
                st.sweep.reclaim_started_ts = None
            elif beyond_down:
                st.sweep.active = True
                st.sweep.direction = "down"
                st.sweep.start_ts = t
                st.sweep.last_beyond_ts = t
                st.sweep.extreme = price
                st.sweep.prints = 1
                st.sweep.valid = False
                st.sweep.reclaim_started_ts = None
            return

        # sweep active: update stats
        if st.sweep.direction == "up":
            if beyond_up:
                st.sweep.last_beyond_ts = t
                st.sweep.extreme = max(st.sweep.extreme, price)
                st.sweep.prints += 1
            # if price is back inside too early, cancel (noise filter)
            if not st.sweep.valid:
                dur = t - st.sweep.start_ts
                if dur >= MIN_SWEEP_DURATION_SEC and st.sweep.prints >= MIN_SWEEP_PRINTS and beyond_up:
                    st.sweep.valid = True
                elif not beyond_up and dur < MIN_SWEEP_DURATION_SEC:
                    st.sweep.reset()

        elif st.sweep.direction == "down":
            if beyond_down:
                st.sweep.last_beyond_ts = t
                st.sweep.extreme = min(st.sweep.extreme, price) if st.sweep.extreme > 0 else price
                st.sweep.prints += 1
            if not st.sweep.valid:
                dur = t - st.sweep.start_ts
                if dur >= MIN_SWEEP_DURATION_SEC and st.sweep.prints >= MIN_SWEEP_PRINTS and beyond_down:
                    st.sweep.valid = True
                elif not beyond_down and dur < MIN_SWEEP_DURATION_SEC:
                    st.sweep.reset()

    def _check_reclaim_signal(self, st: SymbolState, price: float, t: float) -> Optional[str]:
        """
        Returns "Buy" (long) or "Sell" (short) if reclaim is confirmed, else None.
        """
        if not st.sweep.active or not st.sweep.valid:
            return None
        if st.atr <= 0 or st.range_high <= 0 or st.range_low <= 0:
            return None

        # require structure: range wide enough
        if (st.range_high - st.range_low) < (MIN_RANGE_ATR_MULT * st.atr):
            return None

        if st.sweep.direction == "up":
            inside = price <= st.range_high
            if inside and st.sweep.reclaim_started_ts is None:
                st.sweep.reclaim_started_ts = t
            if not inside:
                st.sweep.reclaim_started_ts = None
                return None
            # hold
            if st.sweep.reclaim_started_ts is not None and (t - st.sweep.reclaim_started_ts) >= RECLAIM_HOLD_SEC:
                return "Sell"  # fade: short
        elif st.sweep.direction == "down":
            inside = price >= st.range_low
            if inside and st.sweep.reclaim_started_ts is None:
                st.sweep.reclaim_started_ts = t
            if not inside:
                st.sweep.reclaim_started_ts = None
                return None
            if st.sweep.reclaim_started_ts is not None and (t - st.sweep.reclaim_started_ts) >= RECLAIM_HOLD_SEC:
                return "Buy"   # fade: long
        return None

    # ---------- Execution ----------

    def _enter_trade(self, symbol: str, side: str, st: SymbolState):
        global DAILY_TRADES_TODAY

        # spread check
        bid, ask = st.bid, st.ask
        sp = spread_bps(bid, ask)
        if sp > MAX_SPREAD_BPS:
            log.info(f"[SKIP] {symbol} spread too wide {sp:.2f} bps > {MAX_SPREAD_BPS}")
            return

        # compute entry price estimate
        entry_est = ask if side == "Buy" else bid
        if entry_est <= 0:
            entry_est = st.last_price
        if entry_est <= 0:
            return

        # TP/SL
        tp = st.mid
        atr = st.atr
        if atr <= 0 or tp <= 0:
            return

        if st.sweep.direction == "up":  # short
            sl = st.sweep.extreme + SL_ATR_MULT * atr
        else:  # long
            sl = st.sweep.extreme - SL_ATR_MULT * atr

        # round tp/sl to tick
        rules = self.rules[symbol]
        tp = round_to_tick(tp, rules.tick_size)
        sl = round_to_tick(sl, rules.tick_size)

        # sanity: TP should be in profit direction
        if side == "Sell" and tp >= entry_est:
            log.info(f"[SKIP] {symbol} short TP not below entry (tp={tp}, entry≈{entry_est})")
            return
        if side == "Buy" and tp <= entry_est:
            log.info(f"[SKIP] {symbol} long TP not above entry (tp={tp}, entry≈{entry_est})")
            return

        qty = self._calc_order_qty(symbol, entry=entry_est, stop=sl)
        if qty <= 0:
            log.info(f"[SKIP] {symbol} qty too small after rounding/min qty")
            return

        # concurrency checks
        if self._open_positions_count() >= MAX_CONCURRENT_POSITIONS:
            return

        if not self.can_trade_now():
            return

        # place entry
        log.info(f"[ENTRY] {symbol} side={side} qty={qty} tp={tp} sl={sl} atr={atr:.6f} spread={sp:.2f}bps")
        oid = self.client.place_market_order(symbol, side=side, qty=qty, reduce_only=False)
        if not oid:
            return

        # confirm position (best-effort)
        time.sleep(0.8)
        pos_qty, avg_price, pos_side = self.client.get_position_qty_and_entry(symbol)
        if pos_qty <= 0:
            log.warning(f"[WARN] {symbol} entry not visible in positions yet; continuing anyway")
            pos_qty = qty
            pos_side = side
            avg_price = entry_est

        # slippage guard (best-effort)
        if avg_price > 0 and entry_est > 0:
            slp = abs(avg_price - entry_est) / entry_est * 10000.0
            if slp > MAX_SLIPPAGE_BPS:
                log.warning(f"[WARN] {symbol} slippage {slp:.2f}bps > {MAX_SLIPPAGE_BPS}bps (entry_est={entry_est}, fill={avg_price})")

        # try to set exchange-side TP/SL
        ok = self.client.set_trading_stop(symbol, tp=tp, sl=sl)

        # set internal position state (we'll also do internal exits as backup)
        self._set_position(PositionState(
            symbol=symbol,
            side=pos_side if pos_side in ("Buy", "Sell") else side,
            qty=pos_qty,
            entry_price=avg_price,
            open_ts=time.time(),
            tp=tp,
            sl=sl,
            active=True,
        ))

        DAILY_TRADES_TODAY += 1
        save_daily_state()

        # cooldown per symbol
        st.cooldown_until = time.time() + COOLDOWN_AFTER_SIGNAL_SEC

        # reset sweep to avoid immediate re-triggers
        st.sweep.reset()

        log.info(f"[OK] Entered {symbol} {side} qty={pos_qty} avg={avg_price} tp/sl {'set' if ok else 'NOT set (internal backup)'}")

    def _maybe_exit_internal(self):
        """
        Backup exit logic using live ticker price.
        If exchange-side TP/SL is set, we might still exit here if we detect hits,
        but we try to avoid double-closing by confirming positions.
        """
        with self.pos_lock:
            pos = self.position
        if not pos.active:
            return
        # Grace period after entry (avoid immediate churn)
        if time.time() - pos.open_ts < 1.0:
            return

        st = self.state.get(pos.symbol)
        if not st:
            return
        with st.lock:
            price = st.last_price
            bid, ask = st.bid, st.ask

        if price <= 0:
            return

        # choose a conservative mark for trigger
        px = bid if pos.side == "Sell" else ask
        if px <= 0:
            px = price

        hit_tp = (px <= pos.tp) if pos.side == "Sell" else (px >= pos.tp)
        hit_sl = (px >= pos.sl) if pos.side == "Sell" else (px <= pos.sl)

        if not (hit_tp or hit_sl):
            return

        reason = "TP" if hit_tp else "SL"
        close_side = "Buy" if pos.side == "Sell" else "Sell"
        log.info(f"[EXIT-{reason}] {pos.symbol} closing via internal trigger at px≈{px:.6f}")

        self.client.place_market_order(pos.symbol, side=close_side, qty=pos.qty, reduce_only=True)
        time.sleep(0.8)

        # verify closed
        q, _, _ = self.client.get_position_qty_and_entry(pos.symbol)
        if q <= 0:
            log.info(f"[OK] Position closed: {pos.symbol}")
            self._clear_position()
        else:
            log.warning(f"[WARN] Position still open (size={q}) after close attempt: {pos.symbol}")

    # ---------- Main loop ----------

    
def _connect_ws_public(self):
    """(Re)create and subscribe the public websocket."""
    if self.ws_public:
        try:
            self.ws_public.exit()
        except Exception:
            pass
        self.ws_public = None

    self.ws_public = WebSocket(
        testnet=self.client.testnet,
        channel_type=CATEGORY,
    )
    self.ws_public.ticker_stream(symbol=self.symbols, callback=self._on_ticker)
    self.ws_public.trade_stream(symbol=self.symbols, callback=self._on_trade)
    self.ws_public.kline_stream(interval=KLINE_INTERVAL_MIN, symbol=self.symbols, callback=self._on_kline)

    log.info("[OK] WebSocket subscriptions set (ticker, trade, kline).")

def run(self):
    """Main loop with watchdog + automatic WS reconnect."""
    self._connect_ws_public()

    last_equity_poll = 0.0
    equity = 0.0
    last_dash = 0.0

    WS_STALE_SEC = 20.0
    WS_RECONNECT_BACKOFF_SEC = 2.0
    last_reconnect_attempt = 0.0

    while self.running:
        try:
            now = time.time()

            # --- WebSocket watchdog ---
            all_stale = True
            for s in self.symbols:
                st0 = self.state.get(s)
                if st0 and (now - st0.last_trade_ts) < WS_STALE_SEC:
                    all_stale = False
                    break

            if all_stale and (now - last_reconnect_attempt) > WS_RECONNECT_BACKOFF_SEC:
                log.warning(f"[WS] Stale for >{WS_STALE_SEC:.0f}s. Reconnecting...")
                last_reconnect_attempt = now
                self._connect_ws_public()

            # --- equity / daily kill switch ---
            if now - last_equity_poll > 30:
                equity = self.client.get_equity_usdt()
                init_or_roll_daily_state(equity)
                update_kill_switch(equity)
                if KILL_SWITCH:
                    log.warning(f"[KILL] Trading halted: {KILL_REASON}")
                last_equity_poll = now

            # --- dashboard ---
            if now - last_dash >= DASHBOARD_REFRESH_SEC:
                self.render_dashboard(equity)
                last_dash = now

            # --- internal exits (backup) ---
            if self._has_open_position():
                self._maybe_exit_internal()
                time.sleep(0.1)
                continue

            if not self.can_trade_now():
                time.sleep(0.25)
                continue

            # --- scan symbols ---
            for sym in self.symbols:
                st = self.state[sym]
                with st.lock:
                    if time.time() < st.cooldown_until:
                        continue

                    price = st.last_price
                    t = st.last_trade_ts if st.last_trade_ts > 0 else time.time()

                    if st.atr <= 0 or st.range_high <= 0 or st.range_low <= 0 or st.mid <= 0:
                        continue

                    self._update_sweep_state(st, price, t)

                    sig = self._check_reclaim_signal(st, price, t)
                    if sig is None:
                        continue

                    self._enter_trade(sym, side=sig, st=st)

            time.sleep(0.1)

        except Exception as e:
            log.error(f"[LOOP] Exception: {e}", exc_info=True)
            time.sleep(0.5)
            try:
                self._connect_ws_public()
            except Exception:
                pass

    log.info("[EXIT] shutdown=True")



    def render_dashboard(self, equity: float):
        """Lightweight terminal dashboard (refresh controlled by DASHBOARD_REFRESH_SEC)."""
        try:
            now = time.time()
            utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
            start_eq = DAILY_START_EQUITY if DAILY_START_EQUITY is not None else 0.0
            pnl = equity - float(start_eq)
    
            # WS liveness heuristic: any recent trade update in last 10s
            live = False
            for s in self.symbols:
                st0 = self.state.get(s)
                if st0 and (now - st0.last_trade_ts) < 10:
                    live = True
                    break
            ws_status = "LIVE" if live else "STALE"
    
            trades_today = DAILY_TRADES_TODAY
            max_allowed = MAX_TRADES_PER_DAY
    
            # Clear screen + home cursor
            sys.stdout.write("\033[2J\033[H")
    
            header = (
                f"[Bybit WS Sweep Scalper] UTC: {utc_now} | WS: {ws_status} | "
                f"Testnet: {self.client.testnet} | Kill: {'ON' if KILL_SWITCH else 'OFF'}"
            )
            sys.stdout.write(header + "\n")
            sys.stdout.write(
                f"Equity: {equity:,.2f} | DailyPnL: {pnl:+,.2f} | Trades: {trades_today}/{max_allowed} | "
                f"Pos: {'OPEN' if self.position.active else 'none'}\n"
            )
            if KILL_SWITCH:
                sys.stdout.write(f"KillReason: {KILL_REASON}\n")
    
            sys.stdout.write("\n")
            sys.stdout.write("SYMBOL   LAST        SPREADbps  LATms   ATR      RANGE_LOW     MID         RANGE_HIGH    SWEEP            DUR   RECLAIM        CD\n")
            sys.stdout.write("-" * 110 + "\n")
    
            for sym in self.symbols:
                st = self.state[sym]
                with st.lock:
                    last = st.last_price
                    spr = spread_bps(st.bid, st.ask)
                    lat_ms = int(st.latency_ema_s * 1000)
                    atr = st.atr
                    rl, mid, rh = st.range_low, st.mid, st.range_high
    
                    sw = st.sweep
                    if sw.active:
                        sw_state = f"{(sw.direction or '').upper():<4}({('valid' if sw.valid else 'sweep')})"
                        dur = _fmt_age(now - sw.start_ts) if sw.start_ts > 0 else "--:--"
                        if sw.reclaim_started_ts is None:
                            reclaim = "WAIT"
                        else:
                            reclaim = f"HOLD({_fmt_age(now - sw.reclaim_started_ts)})"
                    else:
                        sw_state = "NONE"
                        dur = "--:--"
                        reclaim = "-"
    
                    cd = max(0.0, st.cooldown_until - now)
                    cd_str = _fmt_age(cd) if cd > 0 else "00:00"
    
                sys.stdout.write(
                    f"{sym:<7} {last:>10.4f} {spr:>9.1f} {lat_ms:>6d} {atr:>7.4f} "
                    f"{rl:>11.4f} {mid:>11.4f} {rh:>11.4f} "
                    f"{sw_state:<15} {dur:>5} {reclaim:<12} {cd_str:>5}\n"
                )
    
            sys.stdout.write("\n")
            if self.position.active:
                pos = self.position
                age = _fmt_age(now - pos.open_ts) if pos.open_ts > 0 else "--:--"
                sys.stdout.write(
                    f"OPEN: {pos.symbol} {pos.side} qty={pos.qty} entry={pos.entry_price} TP={pos.tp} SL={pos.sl} age={age}\n"
                )
            else:
                sys.stdout.write("OPEN: none\n")
    
            sys.stdout.flush()
        except Exception as e:
            # Never let dashboard kill the bot
            log.debug(f"[DASH] render failed: {e}")
    
    
    

    # =============================
    # MAIN
    # =============================
    
def main():
    load_dotenv()

    api_key = os.getenv("BYBIT_API_KEY", "").strip()
    api_secret = os.getenv("BYBIT_API_SECRET", "").strip()
    testnet_raw = os.getenv("BYBIT_TESTNET", "true").strip().lower()
    testnet = testnet_raw in ("1", "true", "yes", "y", "on")

    if not api_key or not api_secret:
        log.warning("[WARN] Missing BYBIT_API_KEY/BYBIT_API_SECRET in environment. Trading will NOT work.")
        # You can still run to verify websockets/market data if keys are missing, but trading will fail.

    client = BybitClient(api_key=api_key, api_secret=api_secret, testnet=testnet)
    bot = SweepScalper(client=client, symbols=SYMBOLS)

    def handle_sig(_signum, _frame):
        log.info("[SIG] Received signal, shutting down...")
        bot.stop()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    bot.bootstrap()
    bot.run()


if __name__ == "__main__":
    main()
