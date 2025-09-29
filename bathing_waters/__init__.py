import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

from .utils import RowCountFilter, build_throttled_session

ROW_COUNT_LIMIT = None


def flatten_id(record):
    return record | {"id": record.get("bathingWater", {}).get("id")}


@dlt.source()
def bathing_waters_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://gw.havochvatten.se/external-public/bathing-waters/v2/",
            "paginator": "single_page",
            "session": build_throttled_session(),
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 5,
            "parallelized": True,
        },
        "resources": [
            {
                "name": "waters",
                "selected": False,
                "endpoint": {"path": "bathing-waters/"},
                "processing_steps": [
                    {"filter": RowCountFilter(max_rows=ROW_COUNT_LIMIT)},
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


__all__ = ["bathing_waters_source"]
