from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from dlt.sources.helpers.requests import Session
from requests_ratelimiter import LimiterAdapter
from urllib3.util.retry import Retry

DEFAULT_RETRY: Retry = Retry(
    total=5,
    backoff_factor=2.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD", "OPTIONS"],
    raise_on_status=False,
    respect_retry_after_header=True,
)


@contextmanager
def throttled_session(
    requests_per_second: float,
    *,
    timeout: float = 30.0,
    raise_for_status: bool = False,
) -> Generator[Session, Any, None]:
    """Yield a rate-limited dlt HTTP session with integrated retry logic."""
    session: Session = Session(timeout=timeout, raise_for_status=raise_for_status)

    limiter_adapter: LimiterAdapter = LimiterAdapter(
        limit_statuses=(429,),
        max_retries=DEFAULT_RETRY,
        per_host=True,
        per_second=requests_per_second,
    )

    session.mount(prefix="http://", adapter=limiter_adapter)
    session.mount(prefix="https://", adapter=limiter_adapter)

    try:
        yield session
    finally:
        session.close()
