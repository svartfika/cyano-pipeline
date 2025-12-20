from collections.abc import Iterator
from typing import Any

import dlt
from dlt.extract import DltResource
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

from ..http import throttled_session

BASE_URL = "https://gw.havochvatten.se/external-public/bathing-waters/v2/"
REQUESTS_PER_SECOND = 8


def _flatten_bathing_water_id(record: dict[str, Any]) -> dict[str, Any]:
    """Extract nested bathingWater.id to top level."""
    bathing_water = record.get("bathingWater", {})
    water_id = bathing_water.get("id")

    if not water_id:
        raise ValueError(f"Record missing bathingWater.id: {record}")

    return record | {"id": water_id}


@dlt.source()
def havochvatten_source() -> Iterator[DltResource]:
    """Havochvatten bathing water API source.
    
    Resources: waters (parent only), profiles (SCD2), results (incremental), forecasts
    """
    with throttled_session(requests_per_second=REQUESTS_PER_SECOND) as session:
        rest_config: RESTAPIConfig = {
            "client": {
                "base_url": BASE_URL,
                "paginator": "single_page",
                "session": session,
            },
            "resource_defaults": {"max_table_nesting": 5},
            "resources": [
                {
                    "name": "waters",
                    "selected": False,  # Parent resource to get IDs
                    "endpoint": {"path": "bathing-waters/"},
                    "processing_steps": [
                        {"map": _flatten_bathing_water_id},
                    ],
                    "write_disposition": "replace",
                },
                {
                    "name": "profiles",
                    "selected": False,  # Slowly-changing, select manually
                    "include_from_parent": ["id"],
                    "endpoint": {
                        "path": "bathing-waters/{resources.waters.id}/profiles/",
                        "data_selector": "$",
                    },
                    "write_disposition": {"disposition": "merge", "strategy": "scd2"},
                },
                {
                    "name": "results",
                    "selected": True,  # Frequent updates, default resource
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
                    "selected": False,  # Frequent updates, select manually
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


__all__ = ["havochvatten_source"]
