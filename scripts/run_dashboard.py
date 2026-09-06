"""Launch the Streamlit dashboard.

python scripts/run_dashboard.py
make dashboard
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Live Streamlit dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8501)
    args = parser.parse_args(argv)

    try:
        from streamlit.web.cli import main as st_main
    except ImportError:
        print(
            "Streamlit extra missing. Install with: pip install -e '.[dashboard]'",
            file=sys.stderr,
        )
        return 1

    app = Path(__file__).resolve().parents[1] / "apps" / "dashboard" / "app.py"
    sys.argv = [
        "streamlit",
        "run",
        str(app),
        "--server.address",
        args.host,
        "--server.port",
        str(args.port),
    ]
    st_main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
