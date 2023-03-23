from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Set, Union

import dagster._check as check
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.unresolved_asset_job_definition import define_asset_job
from dagster._utils.merger import merge_dicts

from ..errors import DagsterInvariantViolationError
from ..instance import DagsterInstance
from ..storage.io_manager import IOManagerDefinition
from ..storage.mem_io_manager import mem_io_manager
from .assets import AssetsDefinition
from .source_asset import SourceAsset

if TYPE_CHECKING:
    from dagster._core.definitions.asset_selection import CoercibleToAssetSelection

    from ..execution.execute_in_process_result import ExecuteInProcessResult


def materialize(
    assets: Sequence[Union[AssetsDefinition, SourceAsset]],
    run_config: Any = None,
    instance: Optional[DagsterInstance] = None,
    resources: Optional[Mapping[str, object]] = None,
    partition_key: Optional[str] = None,
    raise_on_error: bool = True,
    tags: Optional[Mapping[str, str]] = None,
    selection: Optional["CoercibleToAssetSelection"] = None,
) -> "ExecuteInProcessResult":
    """Executes a single-threaded, in-process run which materializes provided assets.

    By default, will materialize assets to the local filesystem.

    Args:
        assets (Sequence[Union[AssetsDefinition, SourceAsset]]):
            The assets to materialize. Can also provide :py:class:`SourceAsset` objects to fill dependencies for asset defs.
        resources (Optional[Mapping[str, object]]):
            The resources needed for execution. Can provide resource instances
            directly, or resource definitions. Note that if provided resources
            conflict with resources directly on assets, an error will be thrown.
        run_config (Optional[Any]): The run config to use for the run that materializes the assets.
        partition_key: (Optional[str])
            The string partition key that specifies the run config to execute. Can only be used
            to select run config for assets with partitioned config.
        tags (Optional[Mapping[str, str]]): Tags for the run.
        selection (Optional[Union[str, Sequence[str], Sequence[AssetKey], Sequence[Union[AssetsDefinition, SourceAsset]], AssetSelection]]):
            A sub-selection of assets to materialize.

            The selected assets must all be included in the assets that are passed to the assets
            argument.

            If not provided, then all assets will be materialized.

            The string "my_asset*" selects my_asset and all downstream assets within the code
            location. A list of strings represents the union of all assets selected by strings
            within the list.

    Returns:
        ExecuteInProcessResult: The result of the execution.
    """
    from dagster._core.definitions.definitions_class import Definitions

    assets = check.sequence_param(assets, "assets", of_type=(AssetsDefinition, SourceAsset))
    instance = check.opt_inst_param(instance, "instance", DagsterInstance)
    partition_key = check.opt_str_param(partition_key, "partition_key")
    resources = check.opt_mapping_param(resources, "resources", key_type=str)

    all_executable_keys: Set[AssetKey] = set()
    for asset in assets:
        if isinstance(asset, AssetsDefinition):
            all_executable_keys = all_executable_keys.union(set(asset.keys))

    JOB_NAME = "__ephemeral_asset_job__"

    defs = Definitions(
        jobs=[define_asset_job(name=JOB_NAME, selection=selection)],
        assets=assets,
        resources=resources,
    )
    return check.not_none(
        defs.get_job_def(JOB_NAME),
        "This should always return a job",
    ).execute_in_process(
        run_config=run_config,
        instance=instance,
        partition_key=partition_key,
        raise_on_error=raise_on_error,
        tags=tags,
    )


def materialize_to_memory(
    assets: Sequence[Union[AssetsDefinition, SourceAsset]],
    run_config: Any = None,
    instance: Optional[DagsterInstance] = None,
    resources: Optional[Mapping[str, object]] = None,
    partition_key: Optional[str] = None,
    raise_on_error: bool = True,
    tags: Optional[Mapping[str, str]] = None,
    selection: Optional["CoercibleToAssetSelection"] = None,
) -> "ExecuteInProcessResult":
    """Executes a single-threaded, in-process run which materializes provided assets in memory.

    Will explicitly use :py:func:`mem_io_manager` for all required io manager
    keys. If any io managers are directly provided using the `resources`
    argument, a :py:class:`DagsterInvariantViolationError` will be thrown.

    Args:
        assets (Sequence[Union[AssetsDefinition, SourceAsset]]):
            The assets to materialize. Can also provide :py:class:`SourceAsset` objects to fill dependencies for asset defs.
        run_config (Optional[Any]): The run config to use for the run that materializes the assets.
        resources (Optional[Mapping[str, object]]):
            The resources needed for execution. Can provide resource instances
            directly, or resource definitions. If provided resources
            conflict with resources directly on assets, an error will be thrown.
        partition_key: (Optional[str])
            The string partition key that specifies the run config to execute. Can only be used
            to select run config for assets with partitioned config.
        tags (Optional[Mapping[str, str]]): Tags for the run.
        selection (Optional[Union[str, Sequence[str], Sequence[AssetKey], Sequence[Union[AssetsDefinition, SourceAsset]], AssetSelection]]):
            A sub-selection of assets to materialize.

            The selected assets must all be included in the assets that are passed to the assets
            argument.

            If not provided, then all assets will be materialized.

            The string "my_asset*" selects my_asset and all downstream assets within the code
            location. A list of strings represents the union of all assets selected by strings
            within the list.

    Returns:
        ExecuteInProcessResult: The result of the execution.
    """
    assets = check.sequence_param(assets, "assets", of_type=(AssetsDefinition, SourceAsset))

    # Gather all resource defs for the purpose of checking io managers.
    resources_dict = resources or {}
    all_resource_keys = set(resources_dict.keys())
    for asset in assets:
        all_resource_keys = all_resource_keys.union(asset.resource_defs.keys())

    io_manager_keys = _get_required_io_manager_keys(assets)
    for io_manager_key in io_manager_keys:
        if io_manager_key in all_resource_keys:
            raise DagsterInvariantViolationError(
                "Attempted to call `materialize_to_memory` with a resource "
                f"provided for io manager key '{io_manager_key}'. Do not "
                "provide resources for io manager keys when calling "
                "`materialize_to_memory`, as it will override io management "
                "behavior for all keys."
            )

    resource_defs = merge_dicts({key: mem_io_manager for key in io_manager_keys}, resources_dict)

    return materialize(
        assets=assets,
        run_config=run_config,
        resources=resource_defs,
        instance=instance,
        partition_key=partition_key,
        raise_on_error=raise_on_error,
        tags=tags,
        selection=selection,
    )


def _get_required_io_manager_keys(
    assets: Sequence[Union[AssetsDefinition, SourceAsset]]
) -> Set[str]:
    io_manager_keys = set()
    for asset in assets:
        for requirement in asset.get_resource_requirements():
            if requirement.expected_type == IOManagerDefinition:
                io_manager_keys.add(requirement.key)
    return io_manager_keys
