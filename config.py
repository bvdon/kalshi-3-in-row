"""
config.py — Central configuration, loaded from .env via python-dotenv.
All settings live here. No hardcoded credentials anywhere else.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from the project root regardless of working directory
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Kalshi ──────────────────────────────────────────────────────────────────
KALSHI_KEY_ID: str = os.environ["KALSHI_KEY_ID"]
KALSHI_KEY_FILE: str = os.environ["KALSHI_KEY_FILE"]
KALSHI_ENV: str = os.getenv("KALSHI_ENV", "prod")

# ── Kraken ──────────────────────────────────────────────────────────────────
KRAKEN_API_KEY: str = os.environ["KRAKEN_API_KEY"]
KRAKEN_API_SECRET: str = os.environ["KRAKEN_API_SECRET"]

# ── Bot ─────────────────────────────────────────────────────────────────────
LOOP_INTERVAL_SECONDS: int = int(os.getenv("LOOP_INTERVAL_SECONDS", "1"))
PAPER_TRADE: bool = os.getenv("PAPER_TRADE", "true").lower() == "true"  # default TRUE — must explicitly set false to go live
DB_PATH: str = os.getenv("DB_PATH", "./trades.db")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# ── Active markets (module paths) ────────────────────────────────────────────
_raw_markets = os.getenv("ACTIVE_MARKETS", "")
ACTIVE_MARKETS: list[str] = [m.strip() for m in _raw_markets.split(",") if m.strip()]
