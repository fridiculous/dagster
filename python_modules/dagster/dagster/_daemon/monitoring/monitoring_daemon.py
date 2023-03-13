import logging
import sys
import time
from typing import Iterator, Optional

from dagster import (
    DagsterInstance,
    _check as check,
)
from dagster._core.events import DagsterEventType
from dagster._core.launcher import WorkerStatus
from dagster._core.storage.pipeline_run import (
    IN_PROGRESS_RUN_STATUSES,
    DagsterRunStatus,
    RunRecord,
    RunsFilter,
)
from dagster._core.workspace.context import IWorkspace, IWorkspaceProcessContext
from dagster._utils import DebugCrashFlags
from dagster._utils.error import SerializableErrorInfo, serializable_error_info_from_exc_info

RESUME_RUN_LOG_MESSAGE = "Launching a new run worker to resume run"


def monitor_starting_run(
    instance: DagsterInstance, run_record: RunRecord, logger: logging.Logger
) -> None:
    run = run_record.dagster_run
    check.invariant(run.status == DagsterRunStatus.STARTING)
    run_stats = instance.get_run_stats(run.run_id)

    launch_time = check.not_none(
        run_stats.launch_time, "Run in status STARTING doesn't have a launch time."
    )
    if time.time() - launch_time >= instance.run_monitoring_start_timeout_seconds:
        msg = (
            f"Run {run.run_id} has been running for {time.time() - launch_time} seconds, which is"
            f" longer than the timeout of {instance.run_monitoring_start_timeout_seconds} seconds"
            " to start. Marking run failed"
        )
        logger.info(msg)
        instance.report_run_failed(run, msg)

    # TODO: consider attempting to resume the run, if the run worker is in a bad status


def count_resume_run_attempts(instance: DagsterInstance, run_id: str) -> int:
    events = instance.all_logs(run_id, of_type=DagsterEventType.ENGINE_EVENT)
    return len([event for event in events if event.message == RESUME_RUN_LOG_MESSAGE])


def monitor_started_run(
    instance: DagsterInstance,
    workspace: IWorkspace,
    run_record: RunRecord,
    logger: logging.Logger,
) -> None:
    run = run_record.dagster_run
    check.invariant(run.status == DagsterRunStatus.STARTED)
    instance.handle_started_run(run_record, logger)
    check_health_result = instance.run_launcher.check_run_worker_health(run)
    if check_health_result.status not in [WorkerStatus.RUNNING, WorkerStatus.SUCCESS]:
        num_prev_attempts = count_resume_run_attempts(instance, run.run_id)
        recheck_run = check.not_none(instance.get_run_by_id(run.run_id))
        status_changed = run.status != recheck_run.status
        if status_changed:
            msg = (
                "Detected run status changed during monitoring loop: "
                f"{run.status} -> {recheck_run.status}, disregarding for now"
            )
            logger.info(msg)
            return
        if num_prev_attempts < instance.run_monitoring_max_resume_run_attempts:
            msg = (
                f"Detected run worker status {check_health_result}. Resuming run {run.run_id} with "
                "a new worker."
            )
            logger.info(msg)
            instance.report_engine_event(msg, run)
            attempt_number = num_prev_attempts + 1
            instance.resume_run(
                run.run_id,
                workspace,
                attempt_number,
            )
        else:
            if instance.run_launcher.supports_resume_run:
                msg = (
                    f"Detected run worker status {check_health_result}. Marking run {run.run_id} as"
                    " failed, because it has surpassed the configured maximum attempts to resume"
                    f" the run: {instance.run_monitoring_max_resume_run_attempts}."
                )
            else:
                msg = (
                    f"Detected run worker status {check_health_result}. Marking run {run.run_id} as"
                    " failed."
                )
            logger.info(msg)
            instance.report_run_failed(run, msg)


def execute_monitoring_iteration(
    workspace_process_context: IWorkspaceProcessContext,
    logger: logging.Logger,
    _debug_crash_flags: Optional[DebugCrashFlags] = None,
) -> Iterator[Optional[SerializableErrorInfo]]:
    instance = workspace_process_context.instance
    check.invariant(
        instance.run_launcher.supports_check_run_worker_health, "Must use a supported run launcher"
    )

    # TODO: consider limiting number of runs to fetch
    run_records = list(
        instance.get_run_records(filters=RunsFilter(statuses=IN_PROGRESS_RUN_STATUSES))
    )

    if not run_records:
        return

    logger.info(f"Collected {len(run_records)} runs for monitoring")
    workspace = workspace_process_context.create_request_context()
    for run_record in run_records:
        try:
            logger.info(f"Checking run {run_record.dagster_run.run_id}")

            if run_record.dagster_run.status == DagsterRunStatus.STARTING:
                monitor_starting_run(instance, run_record, logger)
            elif run_record.dagster_run.status == DagsterRunStatus.STARTED:
                monitor_started_run(instance, workspace, run_record, logger)
            elif run_record.dagster_run.status == DagsterRunStatus.CANCELING:
                # TODO: implement canceling timeouts
                pass
            else:
                check.invariant(False, f"Unexpected run status: {run_record.dagster_run.status}")
        except Exception:
            error_info = serializable_error_info_from_exc_info(sys.exc_info())
            logger.error(f"Hit error while monitoring run {run_record.dagster_run.run_id}: {str(error_info)}")
            yield error_info
        else:
            yield
