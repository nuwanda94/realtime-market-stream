"""Smoke test so `make test` has something to run before domain tests land."""

from realtime_market_stream import __version__


def test_package_version() -> None:
    assert __version__ == "0.1.0"
