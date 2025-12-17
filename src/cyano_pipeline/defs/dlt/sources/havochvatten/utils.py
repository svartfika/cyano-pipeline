from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Session
from requests_ratelimiter import LimiterAdapter
from urllib3.util.retry import Retry


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
    """Builds a throttled dlt Session with rate limiting and retry logic."""
    session: Session = Session(timeout=timeout, raise_for_status=raise_for_status)

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

    user_kwargs: dict[str, Any] = limiter_kwargs or {}
    final_kwargs: dict[str, Any] = {**limiter_defaults, **user_kwargs}
    limiter_adapter: LimiterAdapter = LimiterAdapter(**final_kwargs)

    session.mount("http://", limiter_adapter)
    session.mount("https://", limiter_adapter)

    return session


@contextmanager
def throttled_session(
    *,
    auth_token: str | None = None,
    requests_per_second: float = 2,
    requests_per_minute: int = 15,
    timeout: float | None = 30.0,
    raise_for_status: bool = False,
    retry_strategy: Retry | None = None,
    limiter_kwargs: dict[str, Any] | None = None,
) -> Iterator[requests.Session]:
    """Context manager for a throttled session with automatic cleanup."""
    session = build_throttled_session(
        auth_token=auth_token,
        requests_per_second=requests_per_second,
        requests_per_minute=requests_per_minute,
        timeout=timeout,
        raise_for_status=raise_for_status,
        retry_strategy=retry_strategy,
        limiter_kwargs=limiter_kwargs,
    )
    try:
        yield session
    finally:
        session.close()
