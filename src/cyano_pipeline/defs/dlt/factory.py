from collections.abc import Iterator
from typing import Any, override

import dlt
import duckdb
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
)
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.translator import DagsterDltTranslator, DltResourceTranslatorData
from dagster_duckdb import DuckDBResource
from dlt.extract.source import DltSource
from dlt.pipeline.pipeline import Pipeline
from packaging.version import Version


class DltDuckDBTranslator(DagsterDltTranslator):
    @override
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        dataset = data.pipeline.dataset_name if data.pipeline else "default"
        resource_name = data.resource.name

        return spec.replace_attributes(
            key=AssetKey([dataset, resource_name]),
            tags={**spec.tags, "schema": dataset},
        )


def build_dlt_duckdb_asset(
    source: DltSource,
    pipeline_name: str,
    dataset_name: str,
    group_name: str | None = None,
    translator: DagsterDltTranslator | None = None,
    asset_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    run_kwargs: dict[str, Any] | None = None,
) -> AssetsDefinition:
    """Build a Dagster asset from a DLT source writing to DuckDB."""
    pipeline_config: dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "dataset_name": dataset_name,
        "progress": "log",
        "dev_mode": False,
    }
    if pipeline_kwargs:
        pipeline_config.update(pipeline_kwargs)

    pipeline: Pipeline = dlt.pipeline(**pipeline_config)

    decorator_kwargs: dict[str, Any] = {
        "dlt_source": source,
        "dlt_pipeline": pipeline,
        "dagster_dlt_translator": translator or DltDuckDBTranslator(),
        "name": pipeline_name,
    }

    if group_name is not None:
        decorator_kwargs["group_name"] = group_name

    if asset_kwargs:
        decorator_kwargs.update(asset_kwargs)

    @dlt_assets(**decorator_kwargs)
    def _asset(
        context: AssetExecutionContext,
        dlt_res: DagsterDltResource,
        duckdb_res: DuckDBResource,
    ) -> Iterator[AssetMaterialization | MaterializeResult[Any]]:
        conn_config: dict[str, Any] = {}
        if duckdb_res.connection_config:
            conn_config.update(duckdb_res.connection_config)

        if Version(duckdb.__version__) >= Version("1.0.0"):
            conn_config["custom_user_agent"] = "dagster"

        destination = dlt.destinations.duckdb(
            credentials=duckdb_res.database,
            **conn_config,
        )

        yield from dlt_res.run(
            context=context,
            destination=destination,
            dataset_name=dataset_name,
            **(run_kwargs or {}),
        )

    return _asset
