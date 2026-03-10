"""
main.py — Kalshi bot entry point.

Usage:
  python main.py

Designed for cron-based execution at :00 :15 :30 :45.
Each run resolves the previous cycle result, then enters the new cycle.
Short-lived process — starts, does its job, exits.
"""

import importlib
import sys

import config


def main() -> None:
    print(f"[main] markets={config.ACTIVE_MARKETS}")

    for module_path in config.ACTIVE_MARKETS:
        try:
            module = importlib.import_module(module_path)
            module.run()
        except Exception as e:
            print(f"[main] ERROR in {module_path}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
