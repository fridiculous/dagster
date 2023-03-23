from typing import Dict, List

from dagster import Definitions, asset, job, op, repository, resource
from dagster._config.field_utils import EnvVar
from dagster._config.structured_config import Config, ConfigurableResource
from dagster._core.definitions.definitions_class import BindResourcesToJobs
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.repository_definition import (
    PendingRepositoryDefinition,
    RepositoryDefinition,
)
from dagster._core.definitions.resource_annotation import Resource
from dagster._core.definitions.resource_definition import ResourceDefinition
from dagster._core.execution.context.init import InitResourceContext
from dagster._core.host_representation import (
    ExternalPipelineData,
    external_repository_data_from_def,
)
from dagster._core.host_representation.external_data import NestedResource, NestedResourceType
from dagster._core.snap import PipelineSnapshot


def test_repository_snap_all_props():
    @op
    def noop_op(_):
        pass

    @job
    def noop_job():
        noop_op()

    @repository
    def noop_repo():
        return [noop_job]

    external_repo_data = external_repository_data_from_def(noop_repo)

    assert external_repo_data.name == "noop_repo"
    assert len(external_repo_data.external_pipeline_datas) == 1
    assert isinstance(external_repo_data.external_pipeline_datas[0], ExternalPipelineData)

    pipeline_snapshot = external_repo_data.external_pipeline_datas[0].pipeline_snapshot
    assert isinstance(pipeline_snapshot, PipelineSnapshot)
    assert pipeline_snapshot.name == "noop_job"
    assert pipeline_snapshot.description is None
    assert pipeline_snapshot.tags == {}


def resolve_pending_repo_if_required(definitions: Definitions) -> RepositoryDefinition:
    repo_or_caching_repo = definitions.get_inner_repository_for_loading_process()
    return (
        repo_or_caching_repo.compute_repository_definition()
        if isinstance(repo_or_caching_repo, PendingRepositoryDefinition)
        else repo_or_caching_repo
    )


def test_repository_snap_definitions_resources_basic():
    @asset
    def my_asset(foo: Resource[str]):
        pass

    defs = Definitions(
        assets=[my_asset],
        resources={"foo": ResourceDefinition.hardcoded_resource("wrapped")},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)

    assert len(external_repo_data.external_resource_data) == 1
    assert external_repo_data.external_resource_data[0].name == "foo"
    assert external_repo_data.external_resource_data[0].resource_snapshot.name == "foo"
    assert external_repo_data.external_resource_data[0].resource_snapshot.description is None
    assert external_repo_data.external_resource_data[0].configured_values == {}


def test_repository_snap_definitions_resources_nested() -> None:
    class MyInnerResource(ConfigurableResource):
        a_str: str

    class MyOuterResource(ConfigurableResource):
        inner: MyInnerResource

    inner = MyInnerResource(a_str="wrapped")
    defs = Definitions(
        resources={"foo": MyOuterResource(inner=inner)},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 1

    foo = [data for data in external_repo_data.external_resource_data if data.name == "foo"]

    assert len(foo) == 1

    assert len(foo[0].nested_resources) == 1
    assert "inner" in foo[0].nested_resources
    assert foo[0].nested_resources["inner"] == NestedResource(
        NestedResourceType.ANONYMOUS, "MyInnerResource"
    )


def test_repository_snap_definitions_resources_nested_top_level() -> None:
    class MyInnerResource(ConfigurableResource):
        a_str: str

    class MyOuterResource(ConfigurableResource):
        inner: MyInnerResource

    inner = MyInnerResource(a_str="wrapped")
    defs = Definitions(
        resources={"foo": MyOuterResource(inner=inner), "inner": inner},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 2

    foo = [data for data in external_repo_data.external_resource_data if data.name == "foo"]
    inner = [data for data in external_repo_data.external_resource_data if data.name == "inner"]

    assert len(foo) == 1
    assert len(inner) == 1

    assert len(foo[0].nested_resources) == 1
    assert "inner" in foo[0].nested_resources
    assert foo[0].nested_resources["inner"] == NestedResource(NestedResourceType.TOP_LEVEL, "inner")

    assert len(inner[0].parent_resources) == 1
    assert "foo" in inner[0].parent_resources
    assert inner[0].parent_resources["foo"] == "inner"


def test_repository_snap_definitions_function_style_resources_nested() -> None:
    @resource
    def my_inner_resource() -> str:
        return "foo"

    @resource(required_resource_keys={"inner"})
    def my_outer_resource(context: InitResourceContext) -> str:
        return context.resources.inner + "bar"

    defs = Definitions(
        resources={"foo": my_outer_resource, "inner": my_inner_resource},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 2

    foo = [data for data in external_repo_data.external_resource_data if data.name == "foo"]
    inner = [data for data in external_repo_data.external_resource_data if data.name == "inner"]

    assert len(foo) == 1
    assert len(inner) == 1

    assert len(foo[0].nested_resources) == 1
    assert "inner" in foo[0].nested_resources
    assert foo[0].nested_resources["inner"] == NestedResource(NestedResourceType.TOP_LEVEL, "inner")

    assert len(inner[0].parent_resources) == 1
    assert "foo" in inner[0].parent_resources
    assert inner[0].parent_resources["foo"] == "inner"


def test_repository_snap_definitions_resources_nested_many() -> None:
    class MyInnerResource(ConfigurableResource):
        a_str: str

    class MyOuterResource(ConfigurableResource):
        inner: MyInnerResource

    class MyOutermostResource(ConfigurableResource):
        inner: MyOuterResource

    inner = MyInnerResource(a_str="wrapped")
    outer = MyOuterResource(inner=inner)
    defs = Definitions(
        resources={
            "outermost": MyOutermostResource(inner=outer),
            "outer": outer,
        },
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 2

    outermost = [
        data for data in external_repo_data.external_resource_data if data.name == "outermost"
    ]
    assert len(outermost) == 1

    assert len(outermost[0].nested_resources) == 1
    assert "inner" in outermost[0].nested_resources
    assert outermost[0].nested_resources["inner"] == NestedResource(
        NestedResourceType.TOP_LEVEL, "outer"
    )

    outer = [data for data in external_repo_data.external_resource_data if data.name == "outer"]
    assert len(outer) == 1

    assert len(outer[0].nested_resources) == 1
    assert "inner" in outer[0].nested_resources
    assert outer[0].nested_resources["inner"] == NestedResource(
        NestedResourceType.ANONYMOUS, "MyInnerResource"
    )


def test_repository_snap_definitions_resources_complex():
    class MyStringResource(ConfigurableResource):
        """My description."""

        my_string: str = "bar"

    @asset
    def my_asset(foo: MyStringResource):
        pass

    defs = Definitions(
        assets=[my_asset],
        resources={
            "foo": MyStringResource(
                my_string="baz",
            )
        },
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)

    assert len(external_repo_data.external_resource_data) == 1
    assert external_repo_data.external_resource_data[0].name == "foo"
    assert external_repo_data.external_resource_data[0].resource_snapshot.name == "foo"
    assert (
        external_repo_data.external_resource_data[0].resource_snapshot.description
        == "My description."
    )

    # Ensure we get config snaps for the resource's fields
    assert len(external_repo_data.external_resource_data[0].config_field_snaps) == 1
    snap = external_repo_data.external_resource_data[0].config_field_snaps[0]
    assert snap.name == "my_string"
    assert not snap.is_required
    assert snap.default_value_as_json_str == '"bar"'

    # Ensure we get the configured values for the resource
    assert external_repo_data.external_resource_data[0].configured_values == {
        "my_string": '"baz"',
    }


def test_repository_snap_empty():
    @repository
    def empty_repo():
        return []

    external_repo_data = external_repository_data_from_def(empty_repo)
    assert external_repo_data.name == "empty_repo"
    assert len(external_repo_data.external_pipeline_datas) == 0
    assert len(external_repo_data.external_resource_data) == 0


def test_repository_snap_definitions_env_vars() -> None:
    class MyStringResource(ConfigurableResource):
        my_string: str

    class MyInnerResource(ConfigurableResource):
        my_string: str

    class MyOuterResource(ConfigurableResource):
        inner: MyInnerResource

    class MyInnerConfig(Config):
        my_string: str

    class MyDataStructureResource(ConfigurableResource):
        str_list: List[str]
        str_dict: Dict[str, str]

    class MyResourceWithConfig(ConfigurableResource):
        config: MyInnerConfig
        config_list: List[MyInnerConfig]

    @asset
    def my_asset(foo: MyStringResource):
        pass

    defs = Definitions(
        assets=[my_asset],
        resources={
            "foo": MyStringResource(
                my_string=EnvVar("MY_STRING"),
            ),
            "bar": MyStringResource(
                my_string=EnvVar("MY_STRING"),
            ),
            "baz": MyStringResource(
                my_string=EnvVar("MY_OTHER_STRING"),
            ),
            "qux": MyOuterResource(
                inner=MyInnerResource(
                    my_string=EnvVar("MY_INNER_STRING"),
                ),
            ),
            "quux": MyDataStructureResource(
                str_list=[EnvVar("MY_STRING")],
                str_dict={"foo": EnvVar("MY_STRING"), "bar": EnvVar("MY_OTHER_STRING")},
            ),
            "quuz": MyResourceWithConfig(
                config=MyInnerConfig(
                    my_string=EnvVar("MY_CONFIG_NESTED_STRING"),
                ),
                config_list=[
                    MyInnerConfig(
                        my_string=EnvVar("MY_CONFIG_LIST_NESTED_STRING"),
                    )
                ],
            ),
        },
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.utilized_env_vars

    env_vars = dict(external_repo_data.utilized_env_vars)

    assert len(env_vars) == 5
    assert "MY_STRING" in env_vars
    assert {consumer.name for consumer in env_vars["MY_STRING"]} == {"foo", "bar", "quux"}
    assert "MY_OTHER_STRING" in env_vars
    assert {consumer.name for consumer in env_vars["MY_OTHER_STRING"]} == {"baz", "quux"}
    assert "MY_INNER_STRING" in env_vars
    assert {consumer.name for consumer in env_vars["MY_INNER_STRING"]} == {"qux"}
    assert "MY_CONFIG_NESTED_STRING" in env_vars
    assert {consumer.name for consumer in env_vars["MY_CONFIG_NESTED_STRING"]} == {"quuz"}
    assert "MY_CONFIG_LIST_NESTED_STRING" in env_vars
    assert {consumer.name for consumer in env_vars["MY_CONFIG_LIST_NESTED_STRING"]} == {"quuz"}


def test_repository_snap_definitions_resources_assets_usage() -> None:
    class MyResource(ConfigurableResource):
        a_str: str

    @asset
    def my_asset(foo: MyResource):
        pass

    @asset
    def my_other_asset(foo: MyResource, bar: MyResource):
        pass

    @asset
    def my_third_asset():
        pass

    defs = Definitions(
        assets=[my_asset, my_other_asset, my_third_asset],
        resources={"foo": MyResource(a_str="foo"), "bar": MyResource(a_str="bar")},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 2

    foo = [data for data in external_repo_data.external_resource_data if data.name == "foo"]
    assert len(foo) == 1

    assert sorted(foo[0].asset_keys_using, key=lambda k: "".join(k.path)) == [
        AssetKey("my_asset"),
        AssetKey("my_other_asset"),
    ]

    bar = [data for data in external_repo_data.external_resource_data if data.name == "bar"]
    assert len(bar) == 1

    assert bar[0].asset_keys_using == [
        AssetKey("my_other_asset"),
    ]


def test_repository_snap_definitions_function_style_resources_assets_usage() -> None:
    @resource
    def my_resource() -> str:
        return "foo"

    @asset
    def my_asset(foo: Resource[str]):
        pass

    @asset
    def my_other_asset(foo: Resource[str]):
        pass

    @asset
    def my_third_asset():
        pass

    defs = Definitions(
        assets=[my_asset, my_other_asset, my_third_asset],
        resources={"foo": my_resource},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 1

    foo = external_repo_data.external_resource_data[0]

    assert sorted(foo.asset_keys_using, key=lambda k: "".join(k.path)) == [
        AssetKey("my_asset"),
        AssetKey("my_other_asset"),
    ]


def test_repository_snap_definitions_resources_job_op_usage() -> None:
    class MyResource(ConfigurableResource):
        a_str: str

    @op
    def my_op(foo: MyResource):
        pass

    @op
    def my_other_op(foo: MyResource, bar: MyResource):
        pass

    @op
    def my_third_op():
        pass

    @op
    def my_op_in_other_job(foo: MyResource):
        pass

    @job
    def my_first_job() -> None:
        my_op()
        my_other_op()
        my_third_op()

    @job
    def my_second_job() -> None:
        my_op_in_other_job()

    defs = Definitions(
        jobs=BindResourcesToJobs([my_first_job, my_second_job]),
        resources={"foo": MyResource(a_str="foo"), "bar": MyResource(a_str="bar")},
    )

    repo = resolve_pending_repo_if_required(defs)
    external_repo_data = external_repository_data_from_def(repo)
    assert external_repo_data.external_resource_data

    assert len(external_repo_data.external_resource_data) == 2

    foo = [data for data in external_repo_data.external_resource_data if data.name == "foo"]
    assert len(foo) == 1

    assert foo[0].job_ops_using == {
        "my_first_job": ["my_op", "my_other_op"],
        "my_second_job": ["my_op_in_other_job"],
    }

    bar = [data for data in external_repo_data.external_resource_data if data.name == "bar"]
    assert len(bar) == 1

    assert bar[0].job_ops_using == {
        "my_first_job": ["my_other_op"],
    }
