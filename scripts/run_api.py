"""Run the FastAPI serving layer.

    python scripts/run_api.py --port 8000
    make api
"""

from __future__ import annotations

import argparse
import logging
import sys

from realtime_market_stream.config import get_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve latest ticks / OHLC / anomalies")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args(argv)

    try:
        import uvicorn
    except ImportError:
        print(
            "FastAPI extras missing. Install with: pip install -e '.[api]'",
            file=sys.stderr,
        )
        return 1

    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    uvicorn.run(
        "realtime_market_stream.serving.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=settings.log_level.lower(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
