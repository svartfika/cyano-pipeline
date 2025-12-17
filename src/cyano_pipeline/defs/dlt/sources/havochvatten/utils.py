from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dlt.sources.helpers.requests import Session
from requests_ratelimiter import LimiterAdapter
from urllib3.util.retry import Retry


def build_throttled_session(
    *,
    auth_token: str | None = None,
    headers: dict[str, str] | None = None,
    requests_per_second: float | None = 2.0,
    requests_per_minute: float | None = 15.0,
    timeout: float | None = 30.0,
    raise_for_status: bool = True,
    retry_strategy: Retry | None = None,
    limiter_kwargs: dict[str, Any] | None = None,
) -> Session:
    """Builds a throttled dlt Session with rate limiting and retry logic."""
    session = Session(timeout=timeout, raise_for_status=raise_for_status)

    if headers:
        session.headers.update(headers)
    if auth_token:
        session.headers.update({"Authorization": f"Bearer {auth_token}"})

    if retry_strategy is None:
        retry_strategy = Retry(
            total=5,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )

    _limiter_defaults: dict[str, Any] = {
        "per_second": requests_per_second,
        "per_minute": requests_per_minute,
        "max_retries": retry_strategy,
        "limit_statuses": (429,),
        "per_host": True,
    }

    if limiter_kwargs:
        _limiter_defaults.update(limiter_kwargs)

    limiter_config: dict[str, Any] = {k: v for k, v in _limiter_defaults.items() if v is not None}
    limiter_adapter: LimiterAdapter = LimiterAdapter(**limiter_config)

    session.mount("http://", limiter_adapter)
    session.mount("https://", limiter_adapter)

    return session


@contextmanager
def throttled_session(
    *,
    auth_token: str | None = None,
    headers: dict[str, str] | None = None,
    requests_per_second: float | None = 2.0,
    requests_per_minute: float | None = 15.0,
    timeout: float | None = 30.0,
    raise_for_status: bool = False,
    retry_strategy: Retry | None = None,
    limiter_kwargs: dict[str, Any] | None = None,
) -> Iterator[Session]:
    """Context manager for a throttled dlt session with automatic cleanup."""
    session = build_throttled_session(
        auth_token=auth_token,
        headers=headers,
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
