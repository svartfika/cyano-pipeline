from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any, TypedDict, Unpack, override

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
from packaging.version import Version


class DltDuckDBAssetConfig(TypedDict, total=False):
    """Configuration schema for building DLT DuckDB assets."""

    group_name: str | None
    translator: DagsterDltTranslator | None
    asset_kwargs: dict[str, Any] | None
    pipeline_kwargs: dict[str, Any] | None
    run_kwargs: dict[str, Any] | None
    destination_kwargs: dict[str, Any] | None


_DEFAULT_PIPELINE_KWARGS: dict[str, Any] = {
    "progress": "log",
}


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
    **kwargs: Unpack[DltDuckDBAssetConfig],
) -> AssetsDefinition:
    """Build a Dagster asset from a DLT source writing to DuckDB."""
    config: SimpleNamespace = SimpleNamespace(
        group_name=kwargs.get("group_name"),
        translator=kwargs.get("translator") or DltDuckDBTranslator(),
        asset_kwargs=kwargs.get("asset_kwargs") or {},
        run_kwargs=kwargs.get("run_kwargs") or {},
        destination_kwargs=kwargs.get("destination_kwargs") or {},
        pipeline_kwargs={
            **_DEFAULT_PIPELINE_KWARGS,
            **(kwargs.get("pipeline_kwargs") or {}),
        },
    )

    pipeline: dlt.Pipeline = dlt.pipeline(
        **config.pipeline_kwargs,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )

    extra_asset_args: dict[Any, Any] = {k: v for k, v in config.asset_kwargs.items() if v is not None}

    if config.group_name:
        extra_asset_args["group_name"] = config.group_name

    @dlt_assets(
        dlt_source=source,
        dlt_pipeline=pipeline,
        dagster_dlt_translator=config.translator,
        name=pipeline_name,
        **extra_asset_args,
    )
    def _asset(
        context: AssetExecutionContext,
        dlt_res: DagsterDltResource,
        duckdb_res: DuckDBResource,
    ) -> Iterator[AssetMaterialization | MaterializeResult[Any]]:
        """Executes the DLT ingestion process and yields asset materialization metadata."""
        conn_config: dict[str, Any] = (duckdb_res.connection_config or {}).copy()
        conn_config.update(config.destination_kwargs)

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
            **config.run_kwargs,
        )

    return _asset
