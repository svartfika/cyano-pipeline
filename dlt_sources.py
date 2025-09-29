import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

API_BASE_URL = "https://gw.havochvatten.se//external-public/bathing-waters/v2"


@dlt.source
def bathing_waters():
    config: RESTAPIConfig = {
        "client": {
            "base_url": API_BASE_URL,
            "paginator": "single_page",
        },
        "resources": [
            {
                "name": "waters",
                "endpoint": {
                    "path": "bathing-waters/",
                },
            },
            {
                "name": "profile",
                "endpoint": {
                    "path": "bathing-waters/{resources.waters.bathingWater.id}/",
                },
            },
            {
                "name": "results",
                "endpoint": {
                    "path": "bathing-waters/{resources.waters.bathingWater.id}/results/",
                },
            },
            {
                "name": "forecasts",
                "endpoint": {
                    "path": "forecasts/",
                    "params": {"bathingWaterId": "{resources.waters.bathingWater.id}"},
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def main():
    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb(),
        progress="log",
    )

    pipeline.run(bathing_waters())


if __name__ == "__main__":
    main()
