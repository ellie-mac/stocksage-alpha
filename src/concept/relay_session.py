"""Relay session helper – routes requests through Tailscale relay on domestic Mac.

Relay endpoint: http://100.111.44.98:8765
Protocol: send request to relay URL with X-Relay-Token and X-Target-URL headers.
"""
from __future__ import annotations

import requests
from requests import PreparedRequest

RELAY_URL = "http://100.111.44.98:8765"
RELAY_TOKEN = "asi3g68r7Vo_KfXeGmgp8zepVkBtC8Tt"


def make_relay_session(headers: dict | None = None) -> requests.Session:
    """Create a requests.Session that transparently routes through the relay."""
    s = requests.Session()
    if headers:
        s.headers.update(headers)

    _orig_request = s.request

    def _relay_request(method, url, **kwargs):
        # Merge params into target URL so relay sees the full URL
        params = kwargs.pop("params", None)
        if params:
            prep = PreparedRequest()
            prep.prepare_url(url, params)
            target_url = prep.url
        else:
            target_url = url

        hdrs = dict(kwargs.get("headers") or {})
        hdrs["X-Relay-Token"] = RELAY_TOKEN
        hdrs["X-Target-URL"] = target_url
        kwargs["headers"] = hdrs
        return _orig_request(method, RELAY_URL, **kwargs)

    s.request = _relay_request  # type: ignore[method-assign]
    return s
