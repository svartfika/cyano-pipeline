import dlt
from dlt.sources.config import configspec
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from urllib3 import Retry

from .utils import RowCountFilter, build_throttled_session


RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[408, 429, 500, 502, 503, 504],
    allowed_methods=["GET", "HEAD", "OPTIONS", "POST"],
    connect=3,
    read=3,
    status=3,
    raise_on_status=False,
)


@configspec
class HavochvattenSourceConfig:
    base_url: str = "https://gw.havochvatten.se/external-public/bathing-waters/v2/"
    max_rows: int | None = None
    requests_per_minute: int = 1000
    parallelized: bool = True


def flatten_id(record):
    return record | {"id": record.get("bathingWater", {}).get("id")}


@dlt.source()
def havochvatten_source(config: HavochvattenSourceConfig = dlt.config.value):
    config: RESTAPIConfig = {
        "client": {
            "base_url": config.base_url,
            "paginator": "single_page",
            "session": build_throttled_session(
                requests_per_minute=config.requests_per_minute,
                retry_strategy=RETRY_STRATEGY,
            ),
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 5,
            "parallelized": config.parallelized,
        },
        "resources": [
            {
                "name": "waters",
                "selected": False,
                "endpoint": {"path": "bathing-waters/"},
                "processing_steps": [
                    {"filter": RowCountFilter(max_rows=config.max_rows)},
                    {"map": flatten_id},
                ],
                "write_disposition": "skip",
            },
            {
                "name": "profiles",
                "selected": True,
                "endpoint": {
                    "path": "bathing-waters/{resources.waters.id}/profiles/",
                    "data_selector": "$",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "results",
                "selected": True,
                "endpoint": {
                    "path": "bathing-waters/{resources.waters.id}/results/",
                    "data_selector": "$.results",
                },
                "include_from_parent": ["id"],
            },
            {
                "name": "forecasts",
                "selected": True,
                "endpoint": {
                    "path": "forecasts/",
                    "params": {"bathingWaterId": "{resources.waters.id}"},
                    "data_selector": "$.forecasts",
                },
                "include_from_parent": ["id"],
            },
        ],
    }

    yield from rest_api_resources(RESTAPIConfig(**config))


__all__ = ["havochvatten_source"]
