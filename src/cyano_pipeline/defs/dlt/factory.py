from collections.abc import Iterator, Sequence
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
from dagster._core.storage.db_io_manager import DbTypeHandler
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.translator import DagsterDltTranslator, DltResourceTranslatorData
from dagster_duckdb import DuckDBIOManager, DuckDBResource
from dlt.extract.source import DltSource
from packaging.version import Version

_DEFAULT_PIPELINE_KWARGS: dict[str, Any] = {
    "progress": "log",
}


class DefaultDuckDBIOManager(DuckDBIOManager):
    """Concrete DuckDB IO manager to satisfy Dagster requirements for DLT-managed assets."""

    @staticmethod
    @override
    def type_handlers() -> Sequence[DbTypeHandler[Any]]:
        """Return empty handlers as DLT manages data writing internally."""
        return []


class DltDuckDBTranslator(DagsterDltTranslator):
    """Translates DLT resource metadata into DuckDB-specific Dagster asset specifications."""

    @override
    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        dataset = data.pipeline.dataset_name if data.pipeline else "default"
        return spec.replace_attributes(
            key=AssetKey([dataset, data.resource.name]),
            tags={**spec.tags, "schema": dataset},
        )


def build_dlt_duckdb_asset(
    source: DltSource,
    pipeline_name: str,
    dataset_name: str,
    *,
    duckdb_resource: DuckDBResource,
    dlt_resource: DagsterDltResource,
    group_name: str | None = None,
    translator: DagsterDltTranslator | None = None,
    asset_kwargs: dict[str, Any] | None = None,
    pipeline_kwargs: dict[str, Any] | None = None,
    run_kwargs: dict[str, Any] | None = None,
    destination_kwargs: dict[str, Any] | None = None,
) -> AssetsDefinition:
    """Build a Dagster asset from a DLT source writing to DuckDB."""
    effective_translator: DagsterDltTranslator | DltDuckDBTranslator = translator or DltDuckDBTranslator()
    effective_pipeline_kwargs: dict[str, Any] = {**_DEFAULT_PIPELINE_KWARGS, **(pipeline_kwargs or {})}
    effective_asset_kwargs: dict[str, Any] = asset_kwargs or {}
    effective_run_kwargs: dict[str, Any] = run_kwargs or {}
    effective_dest_kwargs: dict[str, Any] = destination_kwargs or {}

    pipeline = dlt.pipeline(
        **effective_pipeline_kwargs,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )

    if group_name:
        effective_asset_kwargs["group_name"] = group_name

    @dlt_assets(
        dlt_source=source,
        dlt_pipeline=pipeline,
        dagster_dlt_translator=effective_translator,
        name=pipeline_name,
        **effective_asset_kwargs,
    )
    def _asset(
        context: AssetExecutionContext,
        dlt_res: DagsterDltResource,
        duckdb_res: DuckDBResource,
    ) -> Iterator[AssetMaterialization | MaterializeResult[Any]]:
        conn_config = (duckdb_res.connection_config or {}).copy()
        conn_config.update(effective_dest_kwargs)

        if Version(duckdb.__version__) >= Version("1.0.0"):
            conn_config.setdefault("custom_user_agent", "dagster")

        destination = dlt.destinations.duckdb(
            credentials=duckdb_res.database,
            **conn_config,
        )

        yield from dlt_res.run(
            context=context,
            destination=destination,
            dataset_name=dataset_name,
            **effective_run_kwargs,
        )

    auto_io_manager = DefaultDuckDBIOManager(
        database=duckdb_resource.database,
        connection_config=duckdb_resource.connection_config,
    )

    return _asset.with_resources(
        {
            "dlt_res": dlt_resource.get_resource_definition(),
            "duckdb_res": duckdb_resource.get_resource_definition(),
            "io_manager": auto_io_manager.get_resource_definition(),
        }
    )
