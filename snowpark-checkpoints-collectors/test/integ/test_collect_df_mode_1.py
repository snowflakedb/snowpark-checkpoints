#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import decimal
import json
import os
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
import pytest
import pandas as pd
from deepdiff import DeepDiff
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    ShortType,
    StringType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
    ArrayType,
    MapType,
    NullType,
    BinaryType,
)

from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import (
    DATAFRAME_PANDERA_SCHEMA_KEY,
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    DATAFRAME_CUSTOM_DATA_KEY,
    COLUMNS_KEY,
    COLUMN_TYPE_KEY,
    PANDERA_COLUMN_TYPE_KEY,
    STRING_COLUMN_TYPE,
    LONG_COLUMN_TYPE,
    DOUBLE_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    PANDAS_OBJECT_DTYPE,
    PANDAS_INTEGER_DTYPE,
    PANDAS_FLOAT_DTYPE,
    PANDAS_DATETIME_DTYPE,
    PANDAS_BOOLEAN_DTYPE,
    PANDAS_TIMEDELTA_DTYPE,
    BOOLEAN_COLUMN_TYPE,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)
from snowflake.snowpark_checkpoints_collector import Singleton

import tempfile

from snowflake.snowpark_checkpoints_collector.utils.telemetry import (
    get_telemetry_manager,
)

TEST_COLLECT_DF_MODE_1_EXPECTED_DIRECTORY_NAME = "test_collect_df_mode_1_expected"
telemetry_folder = "telemetry"


@pytest.fixture(scope="function")
def output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    telemetry_dir = directory / telemetry_folder

    telemetry_manager = get_telemetry_manager()
    telemetry_manager.set_sc_output_path(telemetry_dir)
    return str(directory)


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def singleton():
    Singleton._instances = {}


@pytest.fixture(scope="session", autouse=True)
def telemetry_testing_mode():
    telemetry_manager = get_telemetry_manager()
    telemetry_manager.sc_is_testing = True
    telemetry_manager.sc_is_enabled = True


def test_collect_dataframe(spark_session, singleton, output_path):
    sample_size = 1.0
    checkpoint_name = "test_full_df"

    pyspark_df = spark_session.createDataFrame(
        [("Raul", 21), ("John", 34), ("Rose", 50)], schema="name string, age integer"
    )

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_dataframe_all_column_types(spark_session, singleton, output_path):
    sample_size = 1.0
    checkpoint_name = "test_full_df_all_column_type"

    day_time_interval_data = timedelta(days=13)
    date_data = date(2000, 1, 1)
    decimal_data = decimal.Decimal("3.141516171819")
    timestamp_data = datetime(2000, 1, 1, 12, 0, 0)
    timestamp_ntz_data = datetime(2000, 1, 1, 12, 53, 0, tzinfo=timezone.utc)
    inner_schema = StructType(
        [
            StructField("inner1", StringType(), False),
            StructField("inner2", LongType(), True),
        ]
    )
    struct_data = {"inner1": "A1", "inner2": 10}

    data_df = [
        [
            True,
            1,
            date_data,
            day_time_interval_data,
            2.10,
            3.11,
            4,
            5,
            6,
            "string1",
            timestamp_data,
            timestamp_ntz_data,
            decimal_data,
            ["A", "B", "C", "D", "E"],
            bytes([0x13, 0x00, 0x00, 0x00, 0x08, 0x00]),
            {
                "C1": "black",
                "C2": "yellow",
                "C3": "orange",
                "C4": "blue",
                "C5": "brown",
            },
            None,
            struct_data,
        ],
        [
            False,
            1,
            date_data,
            day_time_interval_data,
            2.10,
            3.11,
            4,
            5,
            6,
            "string2",
            timestamp_data,
            timestamp_ntz_data,
            decimal_data,
            ["q", "w", "e", "r", "t"],
            bytes([0x13, 0x00]),
            {"FA": "AF", "GA": "AG", "HA": "AH", "WE": "EW"},
            None,
            struct_data,
        ],
        [
            True,
            1,
            date_data,
            day_time_interval_data,
            2.10,
            3.11,
            4,
            5,
            6,
            "string3",
            timestamp_data,
            timestamp_ntz_data,
            decimal_data,
            ["HA", "JA", "KA", "LA", "PA"],
            bytes([0x00, 0x08, 0x00]),
            {"RTA": "ERT"},
            None,
            struct_data,
        ],
    ]

    schema_df = StructType(
        [
            StructField("a", BooleanType(), False),
            StructField("b", ByteType(), False),
            StructField("c", DateType(), False),
            StructField("d", DayTimeIntervalType(), False),
            StructField("e", DoubleType(), False),
            StructField("f", FloatType(), False),
            StructField("g", IntegerType(), False),
            StructField("h", LongType(), False),
            StructField("i", ShortType(), False),
            StructField("j", StringType(), False),
            StructField("m", TimestampType(), False),
            StructField("n", TimestampNTZType(), False),
            StructField("o", DecimalType(15, 13), False),
            StructField("p", ArrayType(StringType(), False), False),
            StructField("q", BinaryType(), False),
            StructField("r", MapType(StringType(), StringType(), False), False),
            StructField("s", NullType(), True),
            StructField("t", inner_schema, False),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data_df, schema=schema_df)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_empty_dataframe_with_schema(spark_session, singleton, output_path):
    sample_size = 1.0
    checkpoint_name = "test_empty_df_with_schema"

    data = []
    columns = StructType(
        [
            StructField("Code", LongType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_empty_dataframe_with_object_column(
    spark_session, singleton, output_path
):
    sample_size = 1.0
    checkpoint_name = "test_empty_df_with_object_column"

    data = []
    columns = StructType(
        [
            StructField("Name", StringType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_dataframe_with_unsupported_pandera_column_type(
    spark_session, singleton, output_path
):
    sample_size = 1.0
    checkpoint_name = "test_dataframe_with_unsupported_pandera_column_type"

    data = [
        ["A1", decimal.Decimal("1.123456789")],
        ["A2", decimal.Decimal("2.12345678")],
        ["A3", decimal.Decimal("3.1234567")],
        ["A4", decimal.Decimal("4.123456")],
        ["A5", decimal.Decimal("5.12345")],
    ]
    columns = StructType(
        [
            StructField("Name", StringType(), True),
            StructField("Value", DecimalType(10, 9), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_dataframe_with_null_values(spark_session, singleton, output_path):
    sample_size = 1.0
    checkpoint_name = "test_df_with_null_values"

    pyspark_df = spark_session.createDataFrame(
        [
            ("Raul", None, True),
            ("John", 23, False),
            ("Rose", 51, False),
            ("Sienna", 35, True),
            (None, None, None),
        ],
        schema="name string, age integer, active boolean",
    )

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_sampled_dataframe(spark_session, singleton, output_path):
    sample_size = 0.1
    checkpoint_name = "test_sampled_df"

    pandas_df = pd.DataFrame(
        {
            "name": ["Peter", "Frank", "Rose", "Arthur", "Gloria"],
            "age": [29, 31, 40, 55, 43],
            "salary": [1000.00, 1100.00, 2100.54, 2200.20, 3100.983],
            "date": [
                pd.Timestamp("2024-11-10"),
                pd.Timestamp("2024-11-01"),
                pd.Timestamp("2024-11-01"),
                pd.Timestamp("2024-11-10"),
                pd.Timestamp("2024-11-20"),
            ],
            "active": [True, True, True, False, False],
            "time": [
                pd.Timedelta(days=11),
                pd.Timedelta(days=20),
                pd.Timedelta(days=20),
                pd.Timedelta(days=11),
                pd.Timedelta(days=1),
            ],
        }
    )

    pyspark_df = spark_session.createDataFrame(pandas_df)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    output_file_path = os.path.join(
        output_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        get_checkpoint_file_name(checkpoint_name),
    )

    assert os.path.exists(output_file_path) is True

    schema_contract_output = open(output_file_path).read()
    schema_contract_output_json = json.loads(schema_contract_output)

    pandera_column_type_collection_expected = [
        PANDAS_OBJECT_DTYPE,
        PANDAS_INTEGER_DTYPE,
        PANDAS_FLOAT_DTYPE,
        PANDAS_DATETIME_DTYPE,
        PANDAS_BOOLEAN_DTYPE,
        PANDAS_TIMEDELTA_DTYPE,
    ]

    collected_schema = schema_contract_output_json[DATAFRAME_PANDERA_SCHEMA_KEY]
    collected_schema_columns_collection = collected_schema[COLUMNS_KEY]
    collected_column_type_collection = []
    for column in collected_schema_columns_collection:
        type = collected_schema_columns_collection[column][PANDERA_COLUMN_TYPE_KEY]
        collected_column_type_collection.append(type)

    assert collected_column_type_collection == pandera_column_type_collection_expected

    custom_column_type_collection_expected = [
        STRING_COLUMN_TYPE,
        LONG_COLUMN_TYPE,
        DOUBLE_COLUMN_TYPE,
        TIMESTAMP_COLUMN_TYPE,
        BOOLEAN_COLUMN_TYPE,
        DAYTIMEINTERVAL_COLUMN_TYPE,
    ]

    collected_schema_columns_collection = schema_contract_output_json[
        DATAFRAME_CUSTOM_DATA_KEY
    ][COLUMNS_KEY]
    collected_column_type_collection = []
    for column in collected_schema_columns_collection:
        type = column[COLUMN_TYPE_KEY]
        collected_column_type_collection.append(type)

    assert collected_column_type_collection == custom_column_type_collection_expected


def test_collect_empty_dataframe_without_schema(spark_session, singleton, output_path):
    checkpoint_name = "test_empty_df_without_schema"
    data = []
    columns = StructType()
    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(
            pyspark_df, checkpoint_name=checkpoint_name, output_path=output_path
        )
    assert "It is not possible to collect an empty DataFrame without schema" == str(
        ex_info.value
    )


def test_collect_dataframe_with_only_null_values(spark_session, singleton, output_path):
    sample_size = 1.0
    checkpoint_name = "test_df_with_only_null_values"

    data = [(None, None, None)]
    columns = StructType(
        [
            StructField("Description", StringType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def test_collect_dataframe_all_column_types_with_null_values(
    spark_session, singleton, output_path
):
    sample_size = 1.0
    checkpoint_name = "test_dataframe_all_column_types_with_null_values"

    day_time_interval_data = timedelta(days=13)
    date_data = date(2000, 1, 1)
    decimal_data = decimal.Decimal("3.141516171819")
    timestamp_data = datetime(2000, 1, 1, 12, 0, 0)
    timestamp_ntz_data = datetime(2000, 1, 1, 12, 53, 0, tzinfo=timezone.utc)
    inner_schema = StructType(
        [
            StructField("inner1", StringType(), False),
            StructField("inner2", LongType(), True),
        ]
    )

    data_df = [
        [
            True,
            1,
            date_data,
            day_time_interval_data,
            2.10,
            3.11,
            4,
            5,
            6,
            "string1",
            timestamp_data,
            timestamp_ntz_data,
            decimal_data,
            ["A", "B", "C", "D", "E"],
            bytes([0x13, 0x00, 0x00, 0x00, 0x08, 0x00]),
            {
                "C1": "black",
                "C2": "yellow",
                "C3": "orange",
                "C4": "blue",
                "C5": "brown",
            },
            None,
            {"inner1": "A1", "inner2": None},
        ],
        [
            False,
            1,
            date_data,
            day_time_interval_data,
            2.10,
            3.11,
            4,
            5,
            6,
            "string2",
            timestamp_data,
            timestamp_ntz_data,
            decimal_data,
            ["q", "w", "e", "r", "t"],
            bytes([0x13, 0x00]),
            {"FA": "AF", "GA": "AG", "HA": "AH", "WE": "EW"},
            None,
            {"inner1": "A1", "inner2": 5},
        ],
        [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    ]

    schema_df = StructType(
        [
            StructField("a", BooleanType(), True),
            StructField("b", ByteType(), True),
            StructField("c", DateType(), True),
            StructField("d", DayTimeIntervalType(), True),
            StructField("e", DoubleType(), True),
            StructField("f", FloatType(), True),
            StructField("g", IntegerType(), True),
            StructField("h", LongType(), True),
            StructField("i", ShortType(), True),
            StructField("j", StringType(), True),
            StructField("m", TimestampType(), True),
            StructField("n", TimestampNTZType(), True),
            StructField("o", DecimalType(15, 13), True),
            StructField("p", ArrayType(StringType(), True), True),
            StructField("q", BinaryType(), True),
            StructField("r", MapType(StringType(), StringType(), True), True),
            StructField("s", NullType(), True),
            StructField("t", inner_schema, True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data_df, schema=schema_df)
    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        sample=sample_size,
        output_path=output_path,
    )

    validate_checkpoint_file_output(output_path, checkpoint_name)


def get_checkpoint_file_name(checkpoint_name) -> str:
    return CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(checkpoint_name)


def validate_checkpoint_file_output(output_path: str, checkpoint_name: str) -> None:
    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    telemetry_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name + "_telemetry"
    )

    schema_contract_expected = get_expected(checkpoint_file_name)
    schema_contract_output = get_output(output_path, checkpoint_file_name)
    telemetry_expected = get_expected(telemetry_file_name)
    telemetry_output = get_output_telemetry(output_path)
    expected_obj = json.loads(schema_contract_expected)
    actual_obj = json.loads(schema_contract_output)

    telemetry_expected_obj = json.loads(telemetry_expected)
    telemetry_output_obj = json.loads(telemetry_output)

    exclude_paths = "root['pandera_schema']['version']"
    exclude_telemetry_paths = [
        "root['timestamp']",
        "root['message']['metadata']['device_id']",
        "root['message']['metadata']",
        "root['message']['driver_version']",
    ]

    diff = DeepDiff(
        expected_obj, actual_obj, ignore_order=True, exclude_paths=[exclude_paths]
    )

    diff_telemetry = DeepDiff(
        telemetry_expected_obj,
        telemetry_output_obj,
        ignore_order=True,
        exclude_paths=exclude_telemetry_paths,
    )

    assert diff == {}
    assert diff_telemetry == {}


def get_output(output_path, file_name) -> str:
    output_file_path = os.path.join(
        output_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        file_name,
    )
    with open(output_file_path) as f:
        return f.read().strip()


def get_output_telemetry(output_path: str) -> str:
    telemetry_directory_path = os.path.join(output_path, telemetry_folder)
    for file in os.listdir(telemetry_directory_path):
        if file.endswith(".json"):
            output_file_path = os.path.join(telemetry_directory_path, file)
            with open(output_file_path) as f:
                return f.read().strip()
    return "{}"


def get_expected(file_name: str) -> str:
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        TEST_COLLECT_DF_MODE_1_EXPECTED_DIRECTORY_NAME,
        file_name,
    )

    with open(expected_file_path) as f:
        return f.read().strip()
