"""
connectors/kraken_ws.py — Kraken real-time WebSocket connector.

Maintains an in-memory order book and rolling trade buffer via Kraken's
public WebSocket API (wss://ws.kraken.com). Runs in a background daemon
thread so it never blocks the main loop.

Usage:
    ws = KrakenWebSocket(pair="XBT/USD")
    ws.start()
    # ... wait a moment for initial snapshot ...
    if ws.is_ready():
        bids = ws.get_bids()
        asks = ws.get_asks()
    ws.stop()
"""

import asyncio
import json
import logging
import math
import threading
import time
from typing import Any, Dict, List, Optional

import websockets

logger = logging.getLogger(__name__)

WS_URL = "wss://ws.kraken.com"
MAX_RETRIES = 5
BASE_BACKOFF = 1.0   # seconds
MAX_TRADES_BUFFER = 2000


class KrakenWebSocket:
    """
    Real-time Kraken WebSocket connector for order book and trade data.

    Attributes:
        pair: Trading pair in Kraken WS format, e.g. "XBT/USD".
        depth: Order book depth to subscribe to.
    """

    def __init__(self, pair: str = "XBT/USD", depth: int = 25) -> None:
        """
        Initialize the connector.

        Args:
            pair: Kraken WS pair, e.g. "XBT/USD".
            depth: Order book depth levels (default 25).
        """
        self.pair = pair
        self.depth = depth

        # Shared state
        self._bids: Dict[float, float] = {}   # price → volume
        self._asks: Dict[float, float] = {}   # price → volume
        self._trades: List[Dict[str, Any]] = []  # rolling trade buffer
        self._ready: bool = False
        self._running: bool = False
        self._lock = threading.Lock()

        # Background thread + event loop
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    # ── Public API ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the WebSocket background thread."""
        if self._thread and self._thread.is_alive():
            logger.debug("KrakenWebSocket already running.")
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="KrakenWS")
        self._thread.start()
        logger.info(f"KrakenWebSocket started for {self.pair}")

    def stop(self) -> None:
        """Cleanly shut down the WebSocket connection and background thread."""
        self._running = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("KrakenWebSocket stopped.")

    def is_ready(self) -> bool:
        """Return True once the initial order book snapshot has been received."""
        return self._ready

    def get_bids(self) -> List[List[float]]:
        """
        Return current bids as [[price, volume], ...] sorted descending by price.

        Returns:
            List of [price, volume] pairs, best bid first.
        """
        with self._lock:
            return sorted(
                [[p, v] for p, v in self._bids.items()],
                key=lambda x: x[0],
                reverse=True,
            )

    def get_asks(self) -> List[List[float]]:
        """
        Return current asks as [[price, volume], ...] sorted ascending by price.

        Returns:
            List of [price, volume] pairs, best ask first.
        """
        with self._lock:
            return sorted(
                [[p, v] for p, v in self._asks.items()],
                key=lambda x: x[0],
            )

    def get_trades(self, window_seconds: int = 180) -> List[Dict[str, Any]]:
        """
        Return recent trades within the given time window.

        Args:
            window_seconds: How far back to look (default 180 = 3 minutes).

        Returns:
            List of dicts with keys: price (float), volume (float),
            side ('buy' or 'sell'), time (unix float).
        """
        cutoff = time.time() - window_seconds
        with self._lock:
            return [t for t in self._trades if t["time"] >= cutoff]

    # ── Internal: event loop ────────────────────────────────────────────────

    def _run_loop(self) -> None:
        """Entry point for the background thread — runs an asyncio event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_with_retry())
        except Exception as e:
            logger.error(f"KrakenWS event loop terminated: {e}")
        finally:
            self._loop.close()

    async def _connect_with_retry(self) -> None:
        """Connect to the WebSocket with exponential backoff on failure."""
        attempt = 0
        while self._running and attempt <= MAX_RETRIES:
            try:
                await self._connect()
            except Exception as e:
                if not self._running:
                    break
                attempt += 1
                if attempt > MAX_RETRIES:
                    logger.error(f"KrakenWS max retries ({MAX_RETRIES}) exceeded. Giving up.")
                    break
                backoff = min(BASE_BACKOFF * (2 ** (attempt - 1)), 60)
                logger.warning(f"KrakenWS disconnected ({e}). Retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
                # Reset ready state on reconnect
                with self._lock:
                    self._ready = False
                    self._bids.clear()
                    self._asks.clear()
                await asyncio.sleep(backoff)

    async def _connect(self) -> None:
        """Establish WebSocket connection, subscribe, and process messages."""
        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
            logger.info(f"KrakenWS connected to {WS_URL}")

            # Subscribe to order book
            await ws.send(json.dumps({
                "event": "subscribe",
                "pair": [self.pair],
                "subscription": {"name": "book", "depth": self.depth},
            }))
            # Subscribe to trade feed
            await ws.send(json.dumps({
                "event": "subscribe",
                "pair": [self.pair],
                "subscription": {"name": "trade"},
            }))

            async for raw in ws:
                if not self._running:
                    break
                try:
                    self._handle_message(raw)
                except Exception as e:
                    logger.debug(f"KrakenWS message error: {e} | raw={raw[:120]}")

    # ── Internal: message handling ─────────────────────────────────────────

    def _handle_message(self, raw: str) -> None:
        """Parse and dispatch an incoming WebSocket message."""
        msg = json.loads(raw)

        # Ignore event-type system messages (heartbeat, subscriptionStatus, etc.)
        if isinstance(msg, dict):
            event = msg.get("event", "")
            if event in ("heartbeat", "systemStatus", "subscriptionStatus", "pong"):
                return
            logger.debug(f"KrakenWS event: {event} | {msg}")
            return

        # Data messages are arrays: [channelID, data, channelName, pair]
        if not isinstance(msg, list) or len(msg) < 4:
            return

        channel_name: str = msg[-2]
        payload = msg[1]

        if channel_name.startswith("book"):
            self._handle_book(payload)
        elif channel_name == "trade":
            self._handle_trade(payload)

    def _handle_book(self, payload: Any) -> None:
        """
        Process a book snapshot or update message.

        Snapshot contains 'as' and 'bs' keys.
        Updates contain 'a' and/or 'b' keys.
        """
        if not isinstance(payload, dict):
            return

        with self._lock:
            # Snapshot
            if "as" in payload:
                self._asks = {
                    float(level[0]): float(level[1])
                    for level in payload["as"]
                    if float(level[1]) > 0
                }
            if "bs" in payload:
                self._bids = {
                    float(level[0]): float(level[1])
                    for level in payload["bs"]
                    if float(level[1]) > 0
                }
                if payload.get("as") is not None or payload.get("bs") is not None:
                    self._ready = True
                    logger.info(f"KrakenWS book snapshot received for {self.pair}")

            # Incremental updates
            if "a" in payload:
                for level in payload["a"]:
                    price, volume = float(level[0]), float(level[1])
                    if volume == 0:
                        self._asks.pop(price, None)
                    else:
                        self._asks[price] = volume

            if "b" in payload:
                for level in payload["b"]:
                    price, volume = float(level[0]), float(level[1])
                    if volume == 0:
                        self._bids.pop(price, None)
                    else:
                        self._bids[price] = volume

    def _handle_trade(self, payload: Any) -> None:
        """
        Process a trade message.

        Each payload is a list of trades:
        [price, volume, time, side, orderType, misc]
        side: "b" = buy, "s" = sell
        """
        if not isinstance(payload, list):
            return

        with self._lock:
            for trade in payload:
                if not isinstance(trade, list) or len(trade) < 4:
                    continue
                self._trades.append({
                    "price": float(trade[0]),
                    "volume": float(trade[1]),
                    "time": float(trade[2]),
                    "side": "buy" if trade[3] == "b" else "sell",
                })

            # Trim buffer
            if len(self._trades) > MAX_TRADES_BUFFER:
                self._trades = self._trades[-MAX_TRADES_BUFFER:]
