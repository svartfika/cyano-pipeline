import logging
from collections.abc import Iterator
from logging import Logger
from typing import Any

import dlt
from dlt.common.configuration import configspec
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from .utils import throttled_session

logging.basicConfig(level=logging.DEBUG)
logger: Logger = logging.getLogger(__name__)


@configspec
class HavochvattenSourceConfig:
    base_url: str = "https://gw.havochvatten.se/external-public/bathing-waters/v2/"
    requests_per_second: float = 8
    requests_per_minute: int = 480


def flatten_id(record: dict[str, Any]) -> dict[str, Any]:
    return record | {"id": record.get("bathingWater", {}).get("id")}


@dlt.source()
def havochvatten_source(config: HavochvattenSourceConfig = dlt.config.value) -> Iterator[DltResource]:
    with throttled_session(
        requests_per_minute=config.requests_per_minute,
        requests_per_second=config.requests_per_second,
    ) as session:
        rest_config: RESTAPIConfig = {
            "client": {
                "base_url": config.base_url,
                "paginator": "single_page",
                "session": session,
            },
            "resource_defaults": {"max_table_nesting": 5},
            "resources": [
                {
                    "name": "waters",
                    "selected": False,
                    "endpoint": {"path": "bathing-waters/"},
                    "processing_steps": [
                        {"map": flatten_id},
                    ],
                    "write_disposition": "replace",
                },
                {
                    "name": "profiles",
                    "selected": False,
                    "include_from_parent": ["id"],
                    "endpoint": {
                        "path": "bathing-waters/{resources.waters.id}/profiles/",
                        "data_selector": "$",
                    },
                    "write_disposition": {"disposition": "merge", "strategy": "scd2"},
                },
                {
                    "name": "results",
                    "selected": False,
                    "include_from_parent": ["id"],
                    "endpoint": {
                        "path": "bathing-waters/{resources.waters.id}/results/",
                        "data_selector": "$.results",
                        "incremental": {
                            "cursor_path": "takenAt",
                            "initial_value": "2000-01-01 00:00:00+00",  # ISO 8601
                        },
                    },
                    "write_disposition": "append",
                },
                {
                    "name": "forecasts",
                    "selected": False,
                    "include_from_parent": ["id"],
                    "endpoint": {
                        "path": "forecasts/",
                        "params": {"bathingWaterId": "{resources.waters.id}"},
                        "data_selector": "$.forecasts",
                    },
                    "write_disposition": "replace",
                },
            ],
        }

        yield from rest_api_resources(RESTAPIConfig(**rest_config))


profiles: DltSource = havochvatten_source().with_resources("profiles")
samples: DltSource = havochvatten_source().with_resources("results")

__all__ = ["havochvatten_source", "profiles", "samples"]
