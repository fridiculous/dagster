import logging
import os
import uuid
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import MagicMock, patch

import pandas
import pytest
from dagster import (
    AssetIn,
    AssetKey,
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    IOManagerDefinition,
    MetadataValue,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    Out,
    StaticPartitionsDefinition,
    TableColumn,
    TableSchema,
    TimeWindowPartitionMapping,
    asset,
    build_input_context,
    build_output_context,
    fs_io_manager,
    instance_for_test,
    job,
    materialize,
    op,
)
from dagster._core.errors import DagsterInvariantViolationError
from dagster._core.storage.db_io_manager import TableSlice
from dagster_snowflake import build_snowflake_io_manager
from dagster_snowflake.resources import SnowflakeConnection
from dagster_snowflake_pandas import SnowflakePandasTypeHandler, snowflake_pandas_io_manager
from dagster_snowflake_pandas.snowflake_pandas_type_handler import (
    _convert_string_to_timestamp,
    _convert_timestamp_to_string,
)
from pandas import DataFrame, Timestamp

resource_config = {
    "database": "database_abc",
    "account": "account_abc",
    "user": "user_abc",
    "password": "password_abc",
    "warehouse": "warehouse_abc",
}

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


SHARED_BUILDKITE_SNOWFLAKE_CONF = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": "BUILDKITE",
    "password": os.getenv("SNOWFLAKE_BUILDKITE_PASSWORD", ""),
}


@contextmanager
def temporary_snowflake_table(schema_name: str, db_name: str) -> Iterator[str]:
    snowflake_config = dict(database=db_name, **SHARED_BUILDKITE_SNOWFLAKE_CONF)
    table_name = "test_io_manager_" + str(uuid.uuid4()).replace("-", "_")
    with SnowflakeConnection(
        snowflake_config, logging.getLogger("temporary_snowflake_table")
    ).get_connection() as conn:
        try:
            yield table_name
        finally:
            conn.cursor().execute(f"drop table {schema_name}.{table_name}")


def test_handle_output():
    handler = SnowflakePandasTypeHandler()
    connection = MagicMock()
    df = DataFrame([{"col1": "a", "col2": 1}])
    output_context = build_output_context(resource_config=resource_config)

    metadata = handler.handle_output(
        output_context,
        TableSlice(
            table="my_table",
            schema="my_schema",
            database="my_db",
            columns=None,
            partition_dimensions=[],
        ),
        df,
        connection,
    )

    assert metadata == {
        "dataframe_columns": MetadataValue.table_schema(
            TableSchema(columns=[TableColumn("col1", "object"), TableColumn("col2", "int64")])
        ),
        "row_count": 1,
    }


def test_load_input():
    with patch(
        "dagster_snowflake_pandas.snowflake_pandas_type_handler.pd.read_sql"
    ) as mock_read_sql:
        connection = MagicMock()
        mock_read_sql.return_value = DataFrame([{"COL1": "a", "COL2": 1}])

        handler = SnowflakePandasTypeHandler()
        input_context = build_input_context()
        df = handler.load_input(
            input_context,
            TableSlice(
                table="my_table",
                schema="my_schema",
                database="my_db",
                columns=None,
                partition_dimensions=[],
            ),
            connection,
        )
        assert mock_read_sql.call_args_list[0][1]["sql"] == "SELECT * FROM my_db.my_schema.my_table"
        assert df.equals(DataFrame([{"col1": "a", "col2": 1}]))


def test_type_conversions():
    # no timestamp data
    no_time = pandas.Series([1, 2, 3, 4, 5])
    converted = _convert_string_to_timestamp(_convert_timestamp_to_string(no_time))

    assert (converted == no_time).all()

    # timestamp data
    with_time = pandas.Series(
        [
            pandas.Timestamp("2017-01-01T12:30:45.35"),
            pandas.Timestamp("2017-02-01T12:30:45.35"),
            pandas.Timestamp("2017-03-01T12:30:45.35"),
        ]
    )
    time_converted = _convert_string_to_timestamp(_convert_timestamp_to_string(with_time))

    assert (with_time == time_converted).all()

    # string that isn't a time
    string_data = pandas.Series(["not", "a", "timestamp"])

    assert (_convert_string_to_timestamp(string_data) == string_data).all()


def test_build_snowflake_pandas_io_manager():
    assert isinstance(
        build_snowflake_io_manager([SnowflakePandasTypeHandler()]), IOManagerDefinition
    )
    # test wrapping decorator to make sure that works as expected
    assert isinstance(snowflake_pandas_io_manager, IOManagerDefinition)


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_io_manager_with_snowflake_pandas():
    with temporary_snowflake_table(
        schema_name="SNOWFLAKE_IO_MANAGER_SCHEMA",
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        # Create a job with the temporary table name as an output, so that it will write to that table
        # and not interfere with other runs of this test

        @op(
            out={
                table_name: Out(
                    io_manager_key="snowflake", metadata={"schema": "SNOWFLAKE_IO_MANAGER_SCHEMA"}
                )
            }
        )
        def emit_pandas_df(_):
            return pandas.DataFrame({"foo": ["bar", "baz"], "quux": [1, 2]})

        @op
        def read_pandas_df(df: pandas.DataFrame):
            assert set(df.columns) == {"foo", "quux"}
            assert len(df.index) == 2

        @job(
            resource_defs={"snowflake": snowflake_pandas_io_manager},
            config={
                "resources": {
                    "snowflake": {
                        "config": {
                            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
                            "database": "TEST_SNOWFLAKE_IO_MANAGER",
                        }
                    }
                }
            },
        )
        def io_manager_test_pipeline():
            read_pandas_df(emit_pandas_df())

        res = io_manager_test_pipeline.execute_in_process()
        assert res.success


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_io_manager_with_snowflake_pandas_timestamp_data():
    schema_name = "SNOWFLAKE_IO_MANAGER_SCHEMA"
    db_name = "TEST_SNOWFLAKE_IO_MANAGER"
    with temporary_snowflake_table(
        schema_name=schema_name,
        db_name=db_name,
    ) as table_name:
        time_df = pandas.DataFrame(
            {
                "foo": ["bar", "baz"],
                "date": [
                    pandas.Timestamp("2017-01-01T12:30:45.350"),
                    pandas.Timestamp("2017-02-01T12:30:45.350"),
                ],
            }
        )

        @op(out={table_name: Out(io_manager_key="snowflake", metadata={"schema": schema_name})})
        def emit_time_df(_):
            return time_df

        @op
        def read_time_df(df: pandas.DataFrame):
            assert set(df.columns) == {"foo", "date"}
            assert (df["date"] == time_df["date"]).all()

        @job(
            resource_defs={"snowflake": snowflake_pandas_io_manager},
            config={
                "resources": {
                    "snowflake": {
                        "config": {
                            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
                            "database": db_name,
                        }
                    }
                }
            },
        )
        def io_manager_timestamp_test_job():
            read_time_df(emit_time_df())

        res = io_manager_timestamp_test_job.execute_in_process()
        assert res.success

        @job(
            resource_defs={"snowflake": snowflake_pandas_io_manager},
            config={
                "resources": {
                    "snowflake": {
                        "config": {
                            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
                            "database": db_name,
                            "time_data_as_string": True,
                        }
                    }
                }
            },
        )
        def io_manager_timestamp_as_string_test_job():
            read_time_df(emit_time_df())

        with pytest.raises(DagsterInvariantViolationError):
            io_manager_timestamp_as_string_test_job.execute_in_process()


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_time_window_partitioned_asset():
    with temporary_snowflake_table(
        schema_name="SNOWFLAKE_IO_MANAGER_SCHEMA",
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        partitions_def = DailyPartitionsDefinition(start_date="2022-01-01")

        @asset(
            partitions_def=partitions_def,
            metadata={"partition_expr": "time"},
            config_schema={"value": str},
            key_prefix="SNOWFLAKE_IO_MANAGER_SCHEMA",
            name=table_name,
        )
        def daily_partitioned(context) -> DataFrame:
            partition = Timestamp(context.asset_partition_key_for_output())
            value = context.op_config["value"]

            return DataFrame(
                {
                    "TIME": [partition, partition, partition],
                    "A": [value, value, value],
                    "B": [4, 5, 6],
                }
            )

        @asset(
            partitions_def=partitions_def,
            key_prefix="SNOWFLAKE_IO_MANAGER_SCHEMA",
            ins={"df": AssetIn(["SNOWFLAKE_IO_MANAGER_SCHEMA", table_name])},
            io_manager_key="fs_io",
        )
        def downstream_partitioned(df) -> None:
            # assert that we only get the columns created in daily_partitioned
            assert len(df.index) == 3

        asset_full_name = f"SNOWFLAKE_IO_MANAGER_SCHEMA__{table_name}"
        snowflake_table_path = f"SNOWFLAKE_IO_MANAGER_SCHEMA.{table_name}"

        snowflake_config = {
            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
            "database": "TEST_SNOWFLAKE_IO_MANAGER",
        }
        snowflake_conn = SnowflakeConnection(
            snowflake_config, logging.getLogger("temporary_snowflake_table")
        )

        snowflake_io_manager = snowflake_pandas_io_manager.configured(snowflake_config)
        resource_defs = {"io_manager": snowflake_io_manager, "fs_io": fs_io_manager}

        materialize(
            [daily_partitioned, downstream_partitioned],
            partition_key="2022-01-01",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "1"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert out_df["A"].tolist() == ["1", "1", "1"]

        materialize(
            [daily_partitioned, downstream_partitioned],
            partition_key="2022-01-02",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "2"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2"]

        materialize(
            [daily_partitioned, downstream_partitioned],
            partition_key="2022-01-01",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "3"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["2", "2", "2", "3", "3", "3"]


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_static_partitioned_asset():
    with temporary_snowflake_table(
        schema_name="SNOWFLAKE_IO_MANAGER_SCHEMA",
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        partitions_def = StaticPartitionsDefinition(["red", "yellow", "blue"])

        @asset(
            partitions_def=partitions_def,
            key_prefix=["SNOWFLAKE_IO_MANAGER_SCHEMA"],
            metadata={"partition_expr": "color"},
            config_schema={"value": str},
            name=table_name,
        )
        def static_partitioned(context) -> DataFrame:
            partition = context.asset_partition_key_for_output()
            value = context.op_config["value"]
            return DataFrame(
                {
                    "COLOR": [partition, partition, partition],
                    "A": [value, value, value],
                    "B": [4, 5, 6],
                }
            )

        @asset(
            partitions_def=partitions_def,
            key_prefix="SNOWFLAKE_IO_MANAGER_SCHEMA",
            ins={"df": AssetIn(["SNOWFLAKE_IO_MANAGER_SCHEMA", table_name])},
            io_manager_key="fs_io",
        )
        def downstream_partitioned(df) -> None:
            # assert that we only get the columns created in static_partitioned
            assert len(df.index) == 3

        asset_full_name = f"SNOWFLAKE_IO_MANAGER_SCHEMA__{table_name}"
        snowflake_table_path = f"SNOWFLAKE_IO_MANAGER_SCHEMA.{table_name}"

        snowflake_config = {
            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
            "database": "TEST_SNOWFLAKE_IO_MANAGER",
        }
        snowflake_conn = SnowflakeConnection(
            snowflake_config, logging.getLogger("temporary_snowflake_table")
        )

        snowflake_io_manager = snowflake_pandas_io_manager.configured(snowflake_config)
        resource_defs = {"io_manager": snowflake_io_manager, "fs_io": fs_io_manager}
        materialize(
            [static_partitioned, downstream_partitioned],
            partition_key="red",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "1"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert out_df["A"].tolist() == ["1", "1", "1"]

        materialize(
            [static_partitioned, downstream_partitioned],
            partition_key="blue",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "2"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2"]

        materialize(
            [static_partitioned, downstream_partitioned],
            partition_key="red",
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "3"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["2", "2", "2", "3", "3", "3"]


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_multi_partitioned_asset():
    with temporary_snowflake_table(
        schema_name="SNOWFLAKE_IO_MANAGER_SCHEMA",
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        partitions_def = MultiPartitionsDefinition(
            {
                "time": DailyPartitionsDefinition(start_date="2022-01-01"),
                "color": StaticPartitionsDefinition(["red", "yellow", "blue"]),
            }
        )

        @asset(
            partitions_def=partitions_def,
            key_prefix=["SNOWFLAKE_IO_MANAGER_SCHEMA"],
            metadata={"partition_expr": {"time": "CAST(time as TIMESTAMP)", "color": "color"}},
            config_schema={"value": str},
            name=table_name,
        )
        def multi_partitioned(context) -> DataFrame:
            partition = context.partition_key.keys_by_dimension
            value = context.op_config["value"]
            return DataFrame(
                {
                    "color": [partition["color"], partition["color"], partition["color"]],
                    "time": [partition["time"], partition["time"], partition["time"]],
                    "a": [value, value, value],
                }
            )

        @asset(
            partitions_def=partitions_def,
            key_prefix="SNOWFLAKE_IO_MANAGER_SCHEMA",
            ins={"df": AssetIn(["SNOWFLAKE_IO_MANAGER_SCHEMA", table_name])},
            io_manager_key="fs_io",
        )
        def downstream_partitioned(df) -> None:
            # assert that we only get the columns created in multi_partitioned
            assert len(df.index) == 3

        asset_full_name = f"SNOWFLAKE_IO_MANAGER_SCHEMA__{table_name}"
        snowflake_table_path = f"SNOWFLAKE_IO_MANAGER_SCHEMA.{table_name}"

        snowflake_config = {
            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
            "database": "TEST_SNOWFLAKE_IO_MANAGER",
        }
        snowflake_conn = SnowflakeConnection(
            snowflake_config, logging.getLogger("temporary_snowflake_table")
        )

        snowflake_io_manager = snowflake_pandas_io_manager.configured(snowflake_config)
        resource_defs = {"io_manager": snowflake_io_manager, "fs_io": fs_io_manager}

        materialize(
            [multi_partitioned, downstream_partitioned],
            partition_key=MultiPartitionKey({"time": "2022-01-01", "color": "red"}),
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "1"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert out_df["A"].tolist() == ["1", "1", "1"]

        materialize(
            [multi_partitioned, downstream_partitioned],
            partition_key=MultiPartitionKey({"time": "2022-01-01", "color": "blue"}),
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "2"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2"]

        materialize(
            [multi_partitioned, downstream_partitioned],
            partition_key=MultiPartitionKey({"time": "2022-01-02", "color": "red"}),
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "3"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2", "3", "3", "3"]

        materialize(
            [multi_partitioned, downstream_partitioned],
            partition_key=MultiPartitionKey({"time": "2022-01-01", "color": "red"}),
            resources=resource_defs,
            run_config={"ops": {asset_full_name: {"config": {"value": "4"}}}},
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["2", "2", "2", "3", "3", "3", "4", "4", "4"]


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_dynamic_partitions():
    with temporary_snowflake_table(
        schema_name="SNOWFLAKE_IO_MANAGER_SCHEMA",
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        dynamic_fruits = DynamicPartitionsDefinition(name="dynamic_fruits")

        @asset(
            partitions_def=dynamic_fruits,
            key_prefix=["SNOWFLAKE_IO_MANAGER_SCHEMA"],
            metadata={"partition_expr": "FRUIT"},
            config_schema={"value": str},
            name=table_name,
        )
        def dynamic_partitioned(context) -> DataFrame:
            partition = context.asset_partition_key_for_output()
            value = context.op_config["value"]
            return DataFrame(
                {
                    "fruit": [partition, partition, partition],
                    "a": [value, value, value],
                }
            )

        @asset(
            partitions_def=dynamic_fruits,
            key_prefix="SNOWFLAKE_IO_MANAGER_SCHEMA",
            ins={"df": AssetIn(["SNOWFLAKE_IO_MANAGER_SCHEMA", table_name])},
            io_manager_key="fs_io",
        )
        def downstream_partitioned(df) -> None:
            # assert that we only get the columns created in dynamic_partitioned
            assert len(df.index) == 3

        asset_full_name = f"SNOWFLAKE_IO_MANAGER_SCHEMA__{table_name}"
        snowflake_table_path = f"SNOWFLAKE_IO_MANAGER_SCHEMA.{table_name}"

        snowflake_config = {
            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
            "database": "TEST_SNOWFLAKE_IO_MANAGER",
        }
        snowflake_conn = SnowflakeConnection(
            snowflake_config, logging.getLogger("temporary_snowflake_table")
        )

        snowflake_io_manager = snowflake_pandas_io_manager.configured(snowflake_config)
        resource_defs = {"io_manager": snowflake_io_manager, "fs_io": fs_io_manager}

        with instance_for_test() as instance:
            instance.add_dynamic_partitions(dynamic_fruits.name, ["apple"])

            materialize(
                [dynamic_partitioned, downstream_partitioned],
                partition_key="apple",
                resources=resource_defs,
                instance=instance,
                run_config={"ops": {asset_full_name: {"config": {"value": "1"}}}},
            )

            out_df = snowflake_conn.execute_query(
                f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
            )
            assert out_df["A"].tolist() == ["1", "1", "1"]

            instance.add_dynamic_partitions(dynamic_fruits.name, ["orange"])

            materialize(
                [dynamic_partitioned, downstream_partitioned],
                partition_key="orange",
                resources=resource_defs,
                instance=instance,
                run_config={"ops": {asset_full_name: {"config": {"value": "2"}}}},
            )

            out_df = snowflake_conn.execute_query(
                f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
            )
            assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2"]

            materialize(
                [dynamic_partitioned, downstream_partitioned],
                partition_key="apple",
                resources=resource_defs,
                instance=instance,
                run_config={"ops": {asset_full_name: {"config": {"value": "3"}}}},
            )

            out_df = snowflake_conn.execute_query(
                f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
            )
            assert sorted(out_df["A"].tolist()) == ["2", "2", "2", "3", "3", "3"]


@pytest.mark.skipif(not IS_BUILDKITE, reason="Requires access to the BUILDKITE snowflake DB")
def test_self_dependent_asset():
    schema = "SNOWFLAKE_IO_MANAGER_SCHEMA"
    with temporary_snowflake_table(
        schema_name=schema,
        db_name="TEST_SNOWFLAKE_IO_MANAGER",
    ) as table_name:
        daily_partitions = DailyPartitionsDefinition(start_date="2023-01-01")

        @asset(
            partitions_def=daily_partitions,
            key_prefix=schema,
            ins={
                "self_dependent_asset": AssetIn(
                    key=AssetKey([schema, table_name]),
                    partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1),
                ),
            },
            metadata={
                "partition_expr": "TO_TIMESTAMP(key)",
            },
            config_schema={"value": str, "last_partition_key": str},
            name=table_name,
        )
        def self_dependent_asset(context, self_dependent_asset: DataFrame) -> DataFrame:
            key = context.asset_partition_key_for_output()

            if not self_dependent_asset.empty:
                assert len(self_dependent_asset.index) == 3
                assert (
                    self_dependent_asset["key"] == context.op_config["last_partition_key"]
                ).all()
            else:
                assert context.op_config["last_partition_key"] == "NA"
            value = context.op_config["value"]
            pd_df = DataFrame(
                {
                    "key": [key, key, key],
                    "a": [value, value, value],
                }
            )

            return pd_df

        asset_full_name = f"{schema}__{table_name}"
        snowflake_table_path = f"{schema}.{table_name}"

        snowflake_config = {
            **SHARED_BUILDKITE_SNOWFLAKE_CONF,
            "database": "TEST_SNOWFLAKE_IO_MANAGER",
        }
        snowflake_conn = SnowflakeConnection(
            snowflake_config, logging.getLogger("temporary_snowflake_table")
        )

        snowflake_io_manager = snowflake_pandas_io_manager.configured(snowflake_config)
        resource_defs = {"io_manager": snowflake_io_manager}

        materialize(
            [self_dependent_asset],
            partition_key="2023-01-01",
            resources=resource_defs,
            run_config={
                "ops": {asset_full_name: {"config": {"value": "1", "last_partition_key": "NA"}}}
            },
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1"]

        materialize(
            [self_dependent_asset],
            partition_key="2023-01-02",
            resources=resource_defs,
            run_config={
                "ops": {
                    asset_full_name: {"config": {"value": "2", "last_partition_key": "2023-01-01"}}
                }
            },
        )

        out_df = snowflake_conn.execute_query(
            f"SELECT * FROM {snowflake_table_path}", use_pandas_result=True, fetch_results=True
        )
        assert sorted(out_df["A"].tolist()) == ["1", "1", "1", "2", "2", "2"]
