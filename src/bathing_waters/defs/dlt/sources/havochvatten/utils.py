from typing import Any
from dlt.sources.helpers import requests
from requests_ratelimiter import LimiterAdapter
from urllib3.util.retry import Retry


class RowCountFilter:
    def __init__(self, max_rows=None):
        self.count = 0
        self.max_rows = max_rows

    def __call__(self, record):
        if self.max_rows and self.count >= self.max_rows:
            return False
        self.count += 1
        return True


def build_throttled_session(
    *,
    auth_token: str | None = None,
    requests_per_second: float = 2,
    requests_per_minute: int = 15,
    timeout: float | None = 30.0,
    raise_for_status: bool = False,
    retry_strategy: Retry | None = None,
    limiter_kwargs: dict[str, Any] | None = None,
) -> requests.Session:
    session = requests.Session(raise_for_status=raise_for_status)

    if auth_token:
        session.headers.update({"Authorization": f"Bearer {auth_token}"})

    if retry_strategy is None:
        retry_strategy = Retry(
            total=5,
            backoff_factor=2.0,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            connect=3,
            read=3,
            status=1,
            raise_on_status=False,
            respect_retry_after_header=True,
        )

    limiter_defaults = {
        "per_second": requests_per_second,
        "per_minute": requests_per_minute,
        "max_retries": retry_strategy,
        "limit_statuses": (429,),
        "per_host": True,
    }

    user_kwargs = limiter_kwargs or {}
    final_kwargs = {**limiter_defaults, **user_kwargs}
    limiter_adapter = LimiterAdapter(**final_kwargs)

    session.mount("http://", limiter_adapter)
    session.mount("https://", limiter_adapter)

    if timeout is not None:
        session.request = lambda *args, **kwargs: session.request(
            *args, timeout=(kwargs.pop("timeout", timeout) if timeout is not None else None), **kwargs
        )

    return session
