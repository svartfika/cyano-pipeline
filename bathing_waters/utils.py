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


def build_throttled_session():
    session = requests.Session(raise_for_status=False)

    retry_strategy = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[408, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS", "POST"],
        connect=3,
        read=3,
        status=3,
        raise_on_status=False,
    )

    adapter = LimiterAdapter(
        per_minute=1000,
        max_retries=retry_strategy,
        per_host=True,
        limit_statuses=(429,),
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
