from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Mapping, NamedTuple, Optional, Sequence

import dagster._check as check
from dagster._annotations import PublicAttr, experimental
from dagster._core.definitions.events import AssetKey
from dagster._core.instance import DynamicPartitionsStore
from dagster._core.storage.pipeline_run import DagsterRun, DagsterRunStatus
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster._serdes.serdes import whitelist_for_serdes
from dagster._utils.error import SerializableErrorInfo

if TYPE_CHECKING:
    from dagster._core.definitions.partition import PartitionSetDefinition


@whitelist_for_serdes(old_storage_names={"JobType"})
class InstigatorType(Enum):
    SCHEDULE = "SCHEDULE"
    SENSOR = "SENSOR"


@whitelist_for_serdes
class SkipReason(NamedTuple("_SkipReason", [("skip_message", PublicAttr[Optional[str]])])):
    """Represents a skipped evaluation, where no runs are requested. May contain a message to indicate
    why no runs were requested.

    Attributes:
        skip_message (Optional[str]): A message displayed in dagit for why this evaluation resulted
            in no requested runs.
    """

    def __new__(cls, skip_message: Optional[str] = None):
        return super(SkipReason, cls).__new__(
            cls,
            skip_message=check.opt_str_param(skip_message, "skip_message"),
        )


@whitelist_for_serdes
class RunRequest(
    NamedTuple(
        "_RunRequest",
        [
            ("run_key", PublicAttr[Optional[str]]),
            ("run_config", PublicAttr[Mapping[str, Any]]),
            ("tags", PublicAttr[Mapping[str, str]]),
            ("job_name", PublicAttr[Optional[str]]),
            ("asset_selection", PublicAttr[Optional[Sequence[AssetKey]]]),
            ("stale_assets_only", PublicAttr[bool]),
            ("partition_key", PublicAttr[Optional[str]]),
        ],
    )
):
    """Represents all the information required to launch a single run.  Must be returned by a
    SensorDefinition or ScheduleDefinition's evaluation function for a run to be launched.

    Attributes:
        run_key (Optional[str]): A string key to identify this launched run. For sensors, ensures that
            only one run is created per run key across all sensor evaluations.  For schedules,
            ensures that one run is created per tick, across failure recoveries. Passing in a `None`
            value means that a run will always be launched per evaluation.
        run_config (Optional[Mapping[str, Any]]: Configuration for the run. If the job has
            a :py:class:`PartitionedConfig`, this value will override replace the config
            provided by it.
        tags (Optional[Dict[str, str]]): A dictionary of tags (string key-value pairs) to attach
            to the launched run.
        job_name (Optional[str]): (Experimental) The name of the job this run request will launch.
            Required for sensors that target multiple jobs.
        asset_selection (Optional[Sequence[AssetKey]]): A sequence of AssetKeys that should be
            launched with this run.
        stale_assets_only (Optional[Sequence[AssetKey]]): Set to true to further narrow the asset
            selection to stale assets. If passed without an asset selection, all stale assets in the
            job will be materialized. If the job does not materialize assets, this flag is ignored.
        partition_key (Optional[str]): The partition key for this run request.
    """

    def __new__(
        cls,
        run_key: Optional[str] = None,
        run_config: Optional[Mapping[str, Any]] = None,
        tags: Optional[Mapping[str, str]] = None,
        job_name: Optional[str] = None,
        asset_selection: Optional[Sequence[AssetKey]] = None,
        stale_assets_only: bool = False,
        partition_key: Optional[str] = None,
    ):
        return super(RunRequest, cls).__new__(
            cls,
            run_key=check.opt_str_param(run_key, "run_key"),
            run_config=check.opt_mapping_param(run_config, "run_config", key_type=str),
            tags=check.opt_mapping_param(tags, "tags", key_type=str, value_type=str),
            job_name=check.opt_str_param(job_name, "job_name"),
            asset_selection=check.opt_nullable_sequence_param(
                asset_selection, "asset_selection", of_type=AssetKey
            ),
            stale_assets_only=check.bool_param(stale_assets_only, "stale_assets_only"),
            partition_key=check.opt_str_param(partition_key, "partition_key"),
        )

    def with_replaced_attrs(self, **kwargs: Any) -> "RunRequest":
        fields = self._asdict()
        for k in fields.keys():
            if k in kwargs:
                fields[k] = kwargs[k]
        return RunRequest(**fields)

    def with_resolved_partition(
        self,
        partition_set: "PartitionSetDefinition",
        current_time: Optional[datetime] = None,
        dynamic_partitions_store: Optional[DynamicPartitionsStore] = None,
    ) -> "RunRequest":
        if self.partition_key is None:
            check.failed(
                "Cannot resolve partition for run request without partition key",
            )

        # Relies on the partition set to throw an error if the partition does not exist
        partition = partition_set.get_partition(
            self.partition_key,
            current_time=current_time,
            dynamic_partitions_store=dynamic_partitions_store,
        )

        get_run_request_tags = lambda partition: (
            {**self.tags, **partition_set.tags_for_partition(partition)}
            if self.tags
            else partition_set.tags_for_partition(partition)
        )

        return self.with_replaced_attrs(
            run_config=self.run_config
            if self.run_config
            else partition_set.run_config_for_partition(partition),
            tags=get_run_request_tags(partition),
        )

    def has_resolved_partition_run_request(self) -> bool:
        # Backcompat run requests yielded via `run_request_for_partition` already have resolved
        # partitioning
        return self.tags.get(PARTITION_NAME_TAG) is not None if self.partition_key else True


@whitelist_for_serdes
class PipelineRunReaction(
    NamedTuple(
        "_PipelineRunReaction",
        [
            ("pipeline_run", Optional[DagsterRun]),
            ("error", Optional[SerializableErrorInfo]),
            ("run_status", Optional[DagsterRunStatus]),
        ],
    )
):
    """Represents a request that reacts to an existing pipeline run. If success, it will report logs
    back to the run.

    Attributes:
        pipeline_run (Optional[PipelineRun]): The pipeline run that originates this reaction.
        error (Optional[SerializableErrorInfo]): user code execution error.
        run_status: (Optional[PipelineRunStatus]): The run status that triggered the reaction.
    """

    def __new__(
        cls,
        pipeline_run: Optional[DagsterRun],
        error: Optional[SerializableErrorInfo] = None,
        run_status: Optional[DagsterRunStatus] = None,
    ):
        return super(PipelineRunReaction, cls).__new__(
            cls,
            pipeline_run=check.opt_inst_param(pipeline_run, "pipeline_run", DagsterRun),
            error=check.opt_inst_param(error, "error", SerializableErrorInfo),
            run_status=check.opt_inst_param(run_status, "run_status", DagsterRunStatus),
        )


@experimental
class SensorResult(
    NamedTuple(
        "_SensorResult",
        [
            ("run_requests", Optional[Sequence[RunRequest]]),
            ("skip_reason", Optional[SkipReason]),
            ("pipeline_run_reactions", Optional[Sequence[PipelineRunReaction]]),
            ("cursor", Optional[str]),
        ],
    )
):
    """The result of a sensor evaluation.

    Attributes:
        run_requests (Optional[Sequence[RunRequest]]): A list
            of run requests to be executed.
        skip_reason (Optional[SkipReason]): A skip message indicating why sensor evaluation was skipped.
        cursor (Optional[str]): The cursor value for this sensor, which will be provided on the
            context for the next sensor evaluation.
        pipeline_run_reactions (Optional[PipelineRunReaction]): A list of pipeline run reactions to be evaluated.
    """

    def __new__(
        cls,
        run_requests: Optional[Sequence[RunRequest]] = None,
        skip_reason: Optional[SkipReason] = None,
        pipeline_run_reactions: Optional[Sequence[PipelineRunReaction]] = None,
        cursor: Optional[str] = None,
    ):
        if not run_requests and not skip_reason and not pipeline_run_reactions:
            check.failed(
                "Must provide at least one of run_requests, skip_reason, or pipeline_run_reactions"
            )

        if skip_reason:
            if len(run_requests if run_requests else []) > 0:
                check.failed(
                    "Expected a single SkipReason or one or more RunRequests: received both "
                    "RunRequest and SkipReason"
                )
            elif pipeline_run_reactions:
                check.failed(
                    "Expected a single SkipReason or one or more PipelineRunReaction: "
                    "received both PipelineRunReaction and SkipReason"
                )

        return super(SensorResult, cls).__new__(
            cls,
            run_requests=check.opt_sequence_param(run_requests, "run_requests", RunRequest),
            skip_reason=check.opt_inst_param(skip_reason, "skip_reason", SkipReason),
            pipeline_run_reactions=check.opt_sequence_param(
                pipeline_run_reactions, "pipeline_run_reactions", PipelineRunReaction
            ),
            cursor=check.opt_str_param(cursor, "cursor"),
        )
