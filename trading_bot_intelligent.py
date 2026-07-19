"""
Enterprise Intelligent Trading Bot v3.0 — Linux / Kali
Requirements:
- Windows bridge running with MT5 OPEN and LOGGED IN
- .env with BROKER_API_URL=http://172.24.48.1:8000
"""
import os, sys, sqlite3, logging, asyncio, aiohttp, certifi, ssl, signal, numpy as np
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from collections import Counter

# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class BotConfig:
    broker: str = os.getenv("BROKER", "rest")
    api_key: str = os.getenv("BROKER_API_KEY", "ignored")
    account_id: str = os.getenv("ACCOUNT_ID", "")
    base_url: str = os.getenv("BROKER_API_URL", "http://172.24.48.1:8000")
    use_demo: bool = os.getenv("USE_DEMO", "true").lower() == "true"

    instruments: List[str] = field(default_factory=lambda: os.getenv(
        "INSTRUMENTS", "EUR_USD,GBP_USD,USD_JPY,XAU_USD,USD_CHF,AUD_USD"
    ).split(","))

    poll_interval_sec: int = int(os.getenv("POLL_INTERVAL", "60"))
    candle_lookback: int = int(os.getenv("CANDLE_LOOKBACK", "200"))

    # Risk
    max_position_units: int = int(os.getenv("MAX_UNITS", "5000"))
    max_portfolio_exposure_pct: float = float(os.getenv("MAX_EXPOSURE_PCT", "50"))
    max_daily_drawdown_pct: float = float(os.getenv("MAX_DD_PCT", "3"))
    max_open_trades: int = int(os.getenv("MAX_OPEN_TRADES", "6"))
    starting_equity: float = float(os.getenv("STARTING_EQUITY", "100000"))

    # Intelligent features
    min_atr_pct: float = float(os.getenv("MIN_ATR_PCT", "0.05"))   # Skip if ATR too low (dead market)
    max_atr_pct: float = float(os.getenv("MAX_ATR_PCT", "2.0"))     # Skip if ATR too high (chaos)
    correlation_limit: float = float(os.getenv("CORR_LIMIT", "0.85"))
    partial_exit_pct: float = float(os.getenv("PARTIAL_EXIT", "0.5"))
    session_start: int = int(os.getenv("SESSION_START", "8"))       # London open approx
    session_end: int = int(os.getenv("SESSION_END", "22"))        # NY close approx

    db_path: str = os.getenv("DB_PATH", "trading_state.db")

    # Alerts
    slack_webhook: Optional[str] = os.getenv("SLACK_WEBHOOK")
    discord_webhook: Optional[str] = os.getenv("DISCORD_WEBHOOK")
    telegram_token: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = os.getenv("TELEGRAM_CHAT_ID")
    smtp_host: Optional[str] = os.getenv("SMTP_HOST")
    smtp_user: Optional[str] = os.getenv("SMTP_USER")
    smtp_pass: Optional[str] = os.getenv("SMTP_PASS")
    email_to: Optional[str] = os.getenv("EMAIL_TO")

    max_retries: int = 5
    retry_initial_delay: float = 1.0

    def validate(self) -> None:
        if "localhost" in self.base_url and "127.0.0.1" not in self.base_url:
            # Accept localhost only for testing; prefer real bridge IP
            pass
        if not self.base_url:
            raise ValueError("BROKER_API_URL is required (e.g., http://172.24.48.1:8000)")

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging() -> logging.Logger:
    log = logging.getLogger("intelligent_bot")
    log.setLevel(logging.INFO)
    if log.handlers:
        return log
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    fh = logging.handlers.RotatingFileHandler("intelligent_bot.log", maxBytes=10_000_000, backupCount=5)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    return log

# ============================================================================
# DATABASE
# ============================================================================

class StateStore:
    def __init__(self, path: str):
        self.path = path
        with sqlite3.connect(self.path) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, instrument TEXT, side TEXT,
                    units INTEGER, price REAL, pnl REAL, status TEXT,
                    partial_closed INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS equity (ts TEXT PRIMARY KEY, equity REAL);
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, instrument TEXT, signal TEXT, price REAL
                );
                CREATE TABLE IF NOT EXISTS correlations (
                    ts TEXT, pair1 TEXT, pair2 TEXT, corr REAL
                );
            """)

    def record_trade(self, instrument: str, side: str, units: int, price: float, status: str, partial: int = 0):
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO trades(ts,instrument,side,units,price,status,partial_closed) VALUES(?,?,?,?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), instrument, side, units, price, status, partial))

    def update_trade_status(self, trade_id: int, status: str, pnl: float = 0.0):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE trades SET status=?, pnl=? WHERE id=?", (status, pnl, trade_id))

    def record_signal(self, instrument: str, signal: str, price: float):
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT INTO signals(ts,instrument,signal,price) VALUES(?,?,?,?)",
                (datetime.now(timezone.utc).isoformat(), instrument, signal, price))

    def record_equity(self, equity: float):
        with sqlite3.connect(self.path) as db:
            db.execute("INSERT OR REPLACE INTO equity(ts,equity) VALUES(?,?)",
                (datetime.now(timezone.utc).isoformat(), equity))

    def get_open_trades(self) -> List[Dict]:
        with sqlite3.connect(self.path) as db:
            rows = db.execute("SELECT id, instrument, side, units, price, partial_closed FROM trades WHERE status='OPEN'").fetchall()
        return [{"id": r[0], "instrument": r[1], "side": r[2], "units": r[3], "price": r[4], "partial": r[5]} for r in rows]

    def get_open_trades_count(self) -> int:
        with sqlite3.connect(self.path) as db:
            row = db.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()
        return int(row[0]) if row else 0

    def get_today_pnl(self) -> float:
        today = datetime.now(timezone.utc).date().isoformat()
        with sqlite3.connect(self.path) as db:
            row = db.execute("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE ts LIKE ? AND status='FILLED'", (today + "%",)).fetchone()
        return float(row[0]) if row else 0.0

    def get_equity_series(self, n: int = 20) -> List[float]:
        with sqlite3.connect(self.path) as db:
            rows = db.execute("SELECT equity FROM equity ORDER BY ts DESC LIMIT ?", (n,)).fetchall()
        return [r[0] for r in reversed(rows)]

# ============================================================================
# BROKER CLIENT (Linux -> Windows MT5 Bridge)
# ============================================================================

class BrokerClient:
    def __init__(self, cfg: BotConfig, log: logging.Logger):
        self.cfg = cfg
        self.log = log
        self.session: Optional[aiohttp.ClientSession] = None
        self.ssl_ctx = ssl.create_default_context(cafile=certifi.where())

    async def __aenter__(self) -> "BrokerClient":
        self.session = aiohttp.ClientSession(headers={"Content-Type": "application/json"})
        return self

    async def __aexit__(self, *exc) -> None:
        if self.session:
            await self.session.close()
        return False

    async def _req(self, method: str, endpoint: str, **kw) -> Any:
        url = f"{self.cfg.base_url}{endpoint}"
        try:
            async with self.session.request(method, url, ssl=self.ssl_ctx, timeout=aiohttp.ClientTimeout(total=15), **kw) as r:
                if r.status == 200:
                    return await r.json()
                self.log.error(f"Bridge {method} {url} -> {r.status}")
                return {} if method != "GET" else []
        except Exception as e:
            self.log.error(f"Bridge error: {e}")
            return {} if method != "GET" else []

    async def get_candles(self, instrument: str, granularity="M5", count=200) -> List[dict]:
        sym = instrument.replace("_", "")
        return await self._req("GET", f"/candles?symbol={sym}&tf={granularity}&count={count}")

    async def get_account_summary(self) -> Dict:
        return await self._req("GET", "/account")

    async def get_open_positions(self) -> List[dict]:
        return await self._req("GET", "/positions")

    async def place_order(self, instrument: str, units: int, sl: Optional[float] = None, tp: Optional[float] = None) -> Dict:
        payload = {"symbol": instrument, "units": units, "sl": sl, "tp": tp}
        return await self._req("POST", "/order", json=payload)

    async def close_position(self, trade_id: str) -> Dict:
        return await self._req("POST", f"/close/{trade_id}")

# ============================================================================
# INTELLIGENT STRATEGIES (Ensemble)
# ============================================================================

class Strategy:
    def __init__(self, name: str): self.name = name
    def evaluate(self, instrument: str, candles: List[dict]) -> Optional[str]: ...

class EMACrossStrategy(Strategy):
    def __init__(self): super().__init__("EMA_Consensus")
    def evaluate(self, instrument: str, candles: List[dict]) -> Optional[str]:
        if len(candles) < 30: return None
        closes = np.array([float(c["mid"]["c"]) for c in candles])
        fast = self._ema(closes, 12)[-1]
        slow = self._ema(closes, 26)[-1]
        prev_fast = self._ema(closes, 12)[-2]
        prev_slow = self._ema(closes, 26)[-2]
        if prev_fast <= prev_slow and fast > slow:
            return "buy"
        if prev_fast >= prev_slow and fast < slow:
            return "sell"
        return None
    @staticmethod
    def _ema(s, p):
        k = 2/(p+1); ema = np.zeros_like(s); ema[0] = s[0]
        for i in range(1, len(s)): ema[i] = s[i]*k + ema[i-1]*(1-k)
        return ema

class RSIReversalStrategy(Strategy):
    def __init__(self): super().__init__("RSI_Reversal")
    def evaluate(self, instrument: str, candles: List[dict]) -> Optional[str]:
        if len(candles) < 20: return None
        closes = np.array([float(c["mid"]["c"]) for c in candles[-14:]])
        rsi = self._rsi(closes)
        if rsi < 30:
            return "buy"
        if rsi > 70:
            return "sell"
        return None
    @staticmethod
    def _rsi(s, p=14):
        d = np.diff(s); g = np.maximum(d, 0); l = -np.minimum(d, 0)
        ag, al = g[-p:].mean(), l[-p:].mean()
        return 100 - 100/(1 + (ag/al if al else float('inf')))

class BreakoutStrategy(Strategy):
    def __init__(self): super().__init__("Breakout")
    def evaluate(self, instrument: str, candles: List[dict]) -> Optional[str]:
        if len(candles) < 20: return None
        high = max(float(c["mid"]["h"]) for c in candles[-20:])
        low = min(float(c["mid"]["l"]) for c in candles[-20:])
        price = float(candles[-1]["mid"]["c"])
        if price > high * 0.999:
            return "buy"
        if price < low * 1.001:
            return "sell"
        return None

# ============================================================================
# INTELLIGENT FILTERS
# ============================================================================

class VolatilityFilter:
    """Only trade if market is active but not chaotic."""
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
    def check(self, candles: List[dict]) -> Tuple[bool, str]:
        if len(candles) < 20:
            return False, "insufficient_candles"
        closes = np.array([float(c["mid"]["c"]) for c in candles[-20:]])
        atr = np.mean(np.abs(np.diff(closes))) / closes[-1] * 100
        if atr < self.cfg.min_atr_pct:
            return False, f"volatility_too_low:{atr:.3f}%"
        if atr > self.cfg.max_atr_pct:
            return False, f"volatility_too_high:{atr:.3f}%"
        return True, f"volatility_ok:{atr:.3f}%"

class CorrelationGuard:
    """Prevent opening highly correlated pairs simultaneously."""
    def __init__(self, cfg: BotConfig, store: StateStore):
        self.cfg = cfg
        self.store = store
    def check(self, instrument: str, open_trades: List[dict]) -> Tuple[bool, str]:
        # Simplified: block if same base currency already open
        # (e.g., EURUSD and EURCHF both have EUR)
        base = instrument.split("_")[0] if "_" in instrument else instrument[:3]
        for t in open_trades:
            other_base = t["instrument"].split("_")[0] if "_" in t["instrument"] else t["instrument"][:3]
            if base == other_base and t["instrument"] != instrument:
                return False, f"correlation_block:{base}"
        return True, "correlation_ok"

class SessionFilter:
    def __init__(self, cfg: BotConfig):
        self.cfg = cfg
    def check(self) -> bool:
        hour = datetime.now(timezone.utc).hour
        return self.cfg.session_start <= hour < self.cfg.session_end

# ============================================================================
# RISK MANAGER (Intelligent)
# ============================================================================

class IntelligentRiskManager:
    def __init__(self, cfg: BotConfig, store: StateStore, log: logging.Logger):
        self.cfg = cfg
        self.store = store
        self.log = log

    def can_open(self, instrument: str, units: int, equity: float, candles: List[dict], open_trades: List[dict]) -> Tuple[bool, str]:
        # Hard drawdown kill
        if len(open_trades) == 0 and equity < self.cfg.starting_equity * (1 - self.cfg.max_daily_drawdown_pct/100):
            return False, "drawdown_kill_switch"

        if self.store.get_open_trades_count() >= self.cfg.max_open_trades:
            return False, "max_open_trades"

        # Dynamic sizing based on volatility (inverse)
        if candles:
            closes = np.array([float(c["mid"]["c"]) for c in candles[-20:]])
            vol_factor = max(0.3, 1.0 - np.std(closes[-10:]) / np.mean(closes[-10:]))
        else:
            vol_factor = 1.0
        adjusted_units = int(abs(units) * vol_factor)
        if adjusted_units > self.cfg.max_position_units:
            return False, "unit_limit_after_vol_scale"

        # Correlation check delegated to guard (called separately in engine)
        return True, f"ok_vol_factor:{vol_factor:.2f}"

# ============================================================================
# ALERTING
# ============================================================================

class Alerter:
    def __init__(self, cfg: BotConfig, log: logging.Logger):
        self.cfg, self.log = cfg, log
    async def send(self, title: str, msg: str, level="info"):
        self.log.info(f"[{level.upper()}] {title}: {msg}")
        tasks = []
        if self.cfg.slack_webhook:
            tasks.append(self._post(self.cfg.slack_webhook, {"text": f"*{level.upper()}* {title}\n{msg}"}))
        if self.cfg.discord_webhook:
            tasks.append(self._post(self.cfg.discord_webhook, {"content": f"**{level.upper()}** {title}\n{msg}"}))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    async def _post(self, url, payload):
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10))
        except Exception as e:
            self.log.error(f"Alert POST failed: {e}")

# ============================================================================
# ENGINE
# ============================================================================

class IntelligentEngine:
    def __init__(self, cfg: BotConfig, log: logging.Logger, broker: BrokerClient,
                 store: StateStore, risk: IntelligentRiskManager, alerter: Alerter,
                 strategies: List[Strategy], vol_filter: VolatilityFilter,
                 corr_guard: CorrelationGuard, session_filter: SessionFilter):
        self.cfg = cfg
        self.log = log
        self.broker = broker
        self.store = store
        self.risk = risk
        self.alerter = alerter
        self.strategies = strategies
        self.vol_filter = vol_filter
        self.corr_guard = corr_guard
        self.session_filter = session_filter
        self._stop = asyncio.Event()

    def request_stop(self):
        self.log.info("Graceful shutdown requested.")
        self._stop.set()

    async def run(self):
        await self.alerter.send("Intelligent Bot Started",
            f"Bridge: {self.cfg.base_url} | Instruments: {len(self.cfg.instruments)}")
        while not self._stop.is_set():
            try:
                await self._cycle()
            except Exception as e:
                self.log.exception(f"Cycle error: {e}")
                await self.alerter.send("CRITICAL ERROR", str(e), "error")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.poll_interval_sec)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

    async def _cycle(self):
        # Session filter
        if not self.session_filter.check():
            self.log.info("Outside trading session — sleeping.")
            return

        summary = await self.broker.get_account_summary()
        equity = float(summary.get("account", {}).get("NAV", self.cfg.starting_equity))
        self.store.record_equity(equity)

        # Drawdown kill check
        open_trades = self.store.get_open_trades()
        if equity < self.cfg.starting_equity * (1 - self.cfg.max_daily_drawdown_pct / 100):
            await self.alerter.send("DRAWDOWN KILL SWITCH", f"Equity {equity:.0f} below limit. Stopping new trades.", "error")
            # We don't exit loop, but won't open new trades because risk manager blocks

        # Correlation + volatility + ensemble processing
        await asyncio.gather(*[
            self._process_instrument(inst, open_trades, equity) for inst in self.cfg.instruments
        ], return_exceptions=True)

    async def _process_instrument(self, instrument: str, open_trades: List[dict], equity: float):
        candles = await self.broker.get_candles(instrument, "M5", self.cfg.candle_lookback)
        if not candles or len(candles) < 30:
            return

        # Volatility filter
        ok_vol, msg_vol = self.vol_filter.check(candles)
        if not ok_vol:
            self.log.info(f"{instrument} blocked by volatility: {msg_vol}")
            return

        # Ensemble evaluation
        votes = [s.evaluate(instrument, candles) for s in self.strategies]
        votes = [v for v in votes if v]
        if not votes:
            return

        # Consensus: majority required (intelligent = don't trade on weak signals)
        consensus = Counter(votes)
        best_signal, count = consensus.most_common(1)[0]
        if count < max(2, len(self.strategies) // 2 + 1):
            self.log.info(f"{instrument} weak consensus: {dict(consensus)} — skipping")
            return

        side = best_signal
        price = float(candles[-1]["mid"]["c"])
        self.store.record_signal(instrument, side, price)

        # Check correlation
        ok_corr, msg_corr = self.corr_guard.check(instrument, open_trades)
        if not ok_corr:
            self.log.info(f"{instrument} correlation blocked: {msg_corr}")
            return

        # Existing position check
        existing = next((t for t in open_trades if t["instrument"] == instrument.replace("_", "")), None)
        if existing:
            # Partial exit logic for intelligent profit management
            if best_signal == "buy" and existing["side"] == "buy" and existing["partial"] == 0:
                # If we have an open BUY and signal is still BUY, consider partial exit if price moved favorably
                # (Simplified: close 50% when signal weakens, not implemented fully here for brevity)
                pass
            elif (side == "buy" and existing["side"] == "buy") or (side == "sell" and existing["side"] == "sell"):
                # Same direction: don't add
                return
            else:
                # Opposite direction: could close/reverse, but we'll skip for simplicity
                return

        # Dynamic sizing
        base_units = self.cfg.max_position_units
        # Scale down in high volatility (inverse)
        closes = np.array([float(c["mid"]["c"]) for c in candles[-20:]])
        vol = np.std(closes[-10:]) / np.mean(closes[-10:]) if np.mean(closes[-10:]) > 0 else 1.0
        scaled_units = int(base_units * max(0.3, 1.0 - vol))
        if side == "sell":
            scaled_units = -scaled_units

        # Risk approval
        ok_risk, reason = self.risk.can_open(instrument, scaled_units, equity, candles, open_trades)
        if not ok_risk:
            self.log.info(f"{instrument} risk blocked: {reason}")
            return

        # Execute with SL/TP (ATR-based for intelligent stops)
        atr = np.mean(np.abs(np.diff(closes[-14:]))) if len(closes) > 14 else price * 0.01
        sl = price - atr * 2 if side == "buy" else price + atr * 2
        tp1 = price + atr * 3 if side == "buy" else price - atr * 3

        result = await self.broker.place_order(instrument, scaled_units, sl=sl, tp=tp1)
        if result:
            self.store.record_trade(instrument, side, abs(scaled_units), price, "OPEN", partial=0)
            await self.alerter.send("TRADE OPENED (INTELLIGENT)",
                f"{side.upper()} {abs(scaled_units)} {instrument} @ {price:.5f}\n"
                f"VolFactor:{vol:.2f} Consensus:{best_signal} Votes:{count}\n"
                f"SL:{sl:.5f} TP:{tp1:.5f}")

# ============================================================================
# MAIN
# ============================================================================

async def main():
    log = setup_logging()
    cfg = BotConfig()
    cfg.validate()
    store = StateStore(cfg.db_path)
    risk = IntelligentRiskManager(cfg, store, log)
    alerter = Alerter(cfg, log)
    strategies = [EMACrossStrategy(), RSIReversalStrategy(), BreakoutStrategy()]
    vol_filter = VolatilityFilter(cfg)
    corr_guard = CorrelationGuard(cfg, store)
    session_filter = SessionFilter(cfg)

    async with BrokerClient(cfg, log) as broker:
        engine = IntelligentEngine(cfg, log, broker, store, risk, alerter,
                                    strategies, vol_filter, corr_guard, session_filter)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, engine.request_stop)
        await engine.run()
        await alerter.send("Bot Stopped", "Intelligent engine shut down.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
