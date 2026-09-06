"""Live Streamlit dashboard over Bronze / Silver / Gold lakehouse files.

streamlit run apps/dashboard/app.py
make dashboard
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow ``streamlit run apps/dashboard/app.py`` without installing first.
_SRC = Path(__file__).resolve().parents[2] / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

try:
    import streamlit as st
except ImportError as exc:  # pragma: no cover - optional extra
    raise SystemExit(
        "Streamlit extra missing. Install with: pip install -e '.[dashboard]'"
    ) from exc

from realtime_market_stream.config import get_settings  # noqa: E402
from realtime_market_stream.serving.dashboard import load_dashboard_payload  # noqa: E402


def _number(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def main() -> None:
    st.set_page_config(page_title="realtime-market-stream", layout="wide")
    settings = get_settings()

    st.title("Market stream dashboard")
    st.caption("Local-first view of ticks, tumbling OHLC, Gold anomalies, and event lag.")

    with st.sidebar:
        st.header("Filters")
        symbol = st.text_input("Symbol filter", value="").strip().upper() or None
        limit = st.slider("Row limit", min_value=20, max_value=500, value=200, step=20)
        st.button("Refresh now")
        st.markdown(f"`APP_ENV={settings.app_env}`")

    payload = load_dashboard_payload(settings=settings, symbol=symbol, limit=limit)
    stats = payload["stats"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ticks", stats["tick_count"])
    c2.metric("Symbols", stats["symbol_count"])
    avg_lag = stats["avg_latency_ms"]
    c3.metric("Avg lag (ms)", f"{avg_lag:.1f}" if avg_lag is not None else "n/a")
    c4.metric("Anomalies", len(payload["anomalies"]))

    ohlc = payload["ohlc"]
    series: dict[str, list[float]] = {}
    for row in reversed(ohlc):
        close = _number(row.get("close"))
        if close is None:
            continue
        key = str(row.get("symbol") or "close")
        series.setdefault(key, []).append(close)
    if series:
        st.subheader("OHLC close")
        st.line_chart(series)

    left, right = st.columns(2)
    with left:
        st.subheader("Latest ticks")
        st.dataframe(payload["ticks"] or payload["silver_ticks"], use_container_width=True)
    with right:
        st.subheader("Gold anomalies")
        st.dataframe(payload["anomalies"], use_container_width=True)

    st.caption(f"Lakehouse root: `{payload['lakehouse_root']}`")


if __name__ == "__main__":
    main()
