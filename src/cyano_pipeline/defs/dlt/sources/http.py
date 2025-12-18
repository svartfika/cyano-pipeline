from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, TypedDict, Unpack

from dlt.sources.helpers.requests import Session
from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from requests.auth import AuthBase
from requests_ratelimiter import LimiterAdapter
from urllib3.util.retry import Retry


class SessionConfig(TypedDict, total=False):
    """Configuration schema for a throttled dlt session."""

    auth: str | AuthBase | AuthConfigBase | None
    headers: dict[str, str] | None
    requests_per_second: float | None
    requests_per_minute: float | None
    timeout: float | None
    raise_for_status: bool
    retry_strategy: Retry | None
    limiter_kwargs: dict[str, Any] | None


_DEFAULT_CONFIG: dict[str, Any] = {
    "timeout": 30.0,
    "raise_for_status": False,
    "requests_per_second": 2.0,
    "requests_per_minute": 15.0,
    "auth": None,
    "headers": None,
    "retry_strategy": None,
    "limiter_kwargs": None,
}

_DEFAULT_RETRY_STRATEGY: Retry = Retry(
    total=5,
    backoff_factor=2.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD", "OPTIONS"],
    raise_on_status=False,
    respect_retry_after_header=True,
)


def build_throttled_session(**kwargs: Unpack[SessionConfig]) -> Session:
    """Create a rate-limited dlt HTTP session with integrated retry and auth logic."""
    config: SimpleNamespace = SimpleNamespace(**{**_DEFAULT_CONFIG, **kwargs})

    session: Session = Session(
        timeout=config.timeout,
        raise_for_status=config.raise_for_status,
    )

    if config.auth:
        if isinstance(config.auth, str):
            session.headers.update({"Authorization": f"Bearer {config.auth}"})
        else:
            session.auth = config.auth

    if config.headers:
        session.headers.update(config.headers)

    retry_strategy: Retry = config.retry_strategy or _DEFAULT_RETRY_STRATEGY

    limiter_args: dict[str, Any] = {
        "per_second": config.requests_per_second,
        "per_minute": config.requests_per_minute,
        "max_retries": retry_strategy,
        "limit_statuses": (429,),
        "per_host": True,
    }

    if config.limiter_kwargs:
        limiter_args.update(config.limiter_kwargs)

    if config.requests_per_second is not None or config.requests_per_minute is not None:
        limiter_config: dict[str, Any] = {k: v for k, v in limiter_args.items() if v is not None}
        limiter_adapter: LimiterAdapter = LimiterAdapter(**limiter_config)
        session.mount("http://", limiter_adapter)
        session.mount("https://", limiter_adapter)

    return session


@contextmanager
def throttled_session(**kwargs: Unpack[SessionConfig]) -> Iterator[Session]:
    """Context manager that yields and automatically closes a throttled dlt HTTP session."""
    session: Session = build_throttled_session(**kwargs)
    try:
        yield session
    finally:
        session.close()
