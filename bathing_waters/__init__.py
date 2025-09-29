import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

DB_NAME = "bathing_waters"
DB_SCHEMA = "raw"

ROW_COUNT_LIMIT = 100


class RowCountFilter:
    def __init__(self, max_rows=None):
        self.count = 0
        self.max_rows = max_rows

    def __call__(self, record):
        if self.max_rows and self.count >= self.max_rows:
            return False
        self.count += 1
        return True


def flatten_id(record):
    return record | {"id": record.get("bathingWater", {}).get("id")}


@dlt.source(name="bathing_waters")
def bathing_waters():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://gw.havochvatten.se//external-public/bathing-waters/v2/",
            "paginator": "single_page",
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

    yield from rest_api_resources(config)


def main():
    pipeline = dlt.pipeline(
        pipeline_name=DB_NAME,  # database
        destination=dlt.destinations.duckdb(),
        dataset_name=DB_SCHEMA,  # schema
        progress="log",
    )

    pipeline.run(bathing_waters())


if __name__ == "__main__":
    main()
