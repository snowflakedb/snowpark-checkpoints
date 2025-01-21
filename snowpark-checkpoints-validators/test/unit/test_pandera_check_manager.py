import pandas as pd
import pytest
from datetime import date, datetime
from pandera import Check, DataFrameSchema, Column
from snowflake.snowpark_checkpoints.utils.pandera_check_manager import (
    PanderaCheckManager,
)

from snowflake.snowpark_checkpoints.utils.constants import (
    COLUMNS_KEY,
    DECIMAL_PRECISION_KEY,
    FALSE_COUNT_KEY,
    FORMAT_KEY,
    MARGIN_ERROR_KEY,
    MEAN_KEY,
    MIN_KEY,
    MAX_KEY,
    DEFAULT_DATE_FORMAT,
    NAME_KEY,
    NULL_COUNT_KEY,
    NULLABLE_KEY,
    ROWS_COUNT_KEY,
    SKIP_ALL,
    TRUE_COUNT_KEY,
    TYPE_KEY,
)


def test_add_boolean_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        TRUE_COUNT_KEY: 2,
        FALSE_COUNT_KEY: 1,
        MARGIN_ERROR_KEY: 0,
        ROWS_COUNT_KEY: 3,
    }
    manager._add_boolean_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [True, True, False]})
    schema.validate(df)


def test_add_boolean_checks_with_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        TRUE_COUNT_KEY: 2,
        FALSE_COUNT_KEY: 1,
        MARGIN_ERROR_KEY: 1,
        ROWS_COUNT_KEY: 3,
    }
    manager._add_boolean_checks("col1", additional_check)
    assert len(schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [True, True, False, False]})
    schema.validate(df)


def test_add_boolean_checks_no_true_count():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {FALSE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0, ROWS_COUNT_KEY: 3}
    manager._add_boolean_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [False, False, False]})
    schema.validate(df)


def test_add_boolean_checks_no_false_count():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {TRUE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0, ROWS_COUNT_KEY: 3}
    manager._add_boolean_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [True, True, True]})
    schema.validate(df)


def test_add_numeric_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MEAN_KEY: 5.0, MARGIN_ERROR_KEY: 1.0}
    manager._add_numeric_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [4.5, 5.5, 5.0]})
    schema.validate(df)


def test_add_numeric_checks_with_decimal_precision():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        MEAN_KEY: 5.0,
        MARGIN_ERROR_KEY: 1.0,
        DECIMAL_PRECISION_KEY: 2,
    }
    manager._add_numeric_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [4.50, 5.55, 5.00]})
    schema.validate(df)


def test_add_numeric_checks_no_mean():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MARGIN_ERROR_KEY: 1.0}
    manager._add_numeric_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [0.5, 1.5, 1.0]})
    schema.validate(df)


def test_add_numeric_checks_no_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MEAN_KEY: 5.0}
    manager._add_numeric_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [5.0, 5.0, 5.0]})
    schema.validate(df)


def test_add_numeric_checks_no_decimal_precision():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MEAN_KEY: 5.0, MARGIN_ERROR_KEY: 1.0}

    manager._add_numeric_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [4.5, 5.5, 5.0]})
    schema.validate(df)


def test_add_date_time_checks_between():
    schema = DataFrameSchema({"date_col": Column(datetime)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        FORMAT_KEY: "%Y-%m-%d",
        MIN_KEY: "2023-01-01",
        MAX_KEY: "2023-12-31",
    }

    manager._add_date_time_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "in_range"
    assert check.statistics["min_value"] == datetime(2023, 1, 1)
    assert check.statistics["max_value"] == datetime(2023, 12, 31)

    df = pd.DataFrame({"date_col": [datetime(2023, 1, 1), datetime(2023, 6, 1)]})
    schema.validate(df)


def test_add_date_time_checks_min_only():
    schema = DataFrameSchema({"date_col": Column(datetime)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {FORMAT_KEY: "%Y-%m-%d", MIN_KEY: "2023-01-01"}

    manager._add_date_time_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "greater_than_or_equal_to"
    assert check.statistics["min_value"] == datetime(2023, 1, 1)

    df = pd.DataFrame({"date_col": [datetime(2023, 1, 1), datetime(2023, 6, 1)]})
    schema.validate(df)


def test_add_date_time_checks_max_only():
    schema = DataFrameSchema({"date_col": Column(datetime)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {FORMAT_KEY: "%Y-%m-%d", MAX_KEY: "2023-12-31"}

    manager._add_date_time_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "less_than_or_equal_to"
    assert check.statistics["max_value"] == datetime(2023, 12, 31)

    df = pd.DataFrame({"date_col": [datetime(2023, 1, 1), datetime(2023, 6, 1)]})
    schema.validate(df)


def test_add_date_time_checks_default_format():
    schema = DataFrameSchema({"date_col": Column(datetime)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MIN_KEY: "2023-01-01", MAX_KEY: "2023-12-31"}

    manager._add_date_time_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "in_range"
    assert check.statistics["min_value"] == datetime.strptime(
        "2023-01-01", DEFAULT_DATE_FORMAT
    )
    assert check.statistics["max_value"] == datetime.strptime(
        "2023-12-31", DEFAULT_DATE_FORMAT
    )

    df = pd.DataFrame(
        {
            "date_col": [
                datetime(2023, 1, 1),
                datetime(2023, 6, 1),
                datetime(2023, 12, 22),
            ]
        }
    )
    schema.validate(df)


def test_skip_specific_check():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(50), Check.less_than(50)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    skip_checks = {"col1": ["greater_than"]}

    manager.skip_checks_on_schema(skip_checks)

    assert len(schema.columns["col1"].checks) == 1
    assert schema.columns["col1"].checks[0].name == "less_than"
    assert len(schema.columns["col2"].checks) == 1

    df = pd.DataFrame({"col1": [10, 20, 30], "col2": [True, False, True]})
    schema.validate(df)


def test_skip_checks_on_nonexistent_column():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(40)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    skip_checks = {"col3": [SKIP_ALL]}

    manager.skip_checks_on_schema(skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert len(schema.columns["col2"].checks) == 1

    df = pd.DataFrame({"col1": [10, 20, 30], "col2": [True, False, True]})
    schema.validate(df)


def test_skip_no_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(40)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    skip_checks = {}

    manager.skip_checks_on_schema(skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert len(schema.columns["col2"].checks) == 1

    df = pd.DataFrame({"col1": [10, 20, 30], "col2": [True, False, True]})
    schema.validate(df)


def test_add_custom_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
            "col2": Column(float, checks=[Check.less_than(10.0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_checks = {
        "col1": [Check.less_than(100)],
        "col2": [Check.greater_than(0.0)],
    }

    manager.add_custom_checks(custom_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert schema.columns["col1"].checks[1].name == "less_than"
    assert len(schema.columns["col2"].checks) == 2
    assert schema.columns["col2"].checks[1].name == "greater_than"

    df = pd.DataFrame({"col1": [10, 20, 30], "col2": [1.0, 2.0, 3.0]})
    schema.validate(df)


def test_add_custom_checks_nonexistent_column():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_checks = {
        "col2": [Check.less_than(100)],
    }

    with pytest.raises(ValueError, match="Column col2 not found in schema"):
        manager.add_custom_checks(custom_checks)


def test_add_custom_checks_empty():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_checks = {}

    result_schema = manager.add_custom_checks(custom_checks)

    assert result_schema == schema
    assert len(schema.columns["col1"].checks) == 1
    assert schema.columns["col1"].checks[0].name == "greater_than"

    df = pd.DataFrame({"col1": [10, 20, 30]})
    schema.validate(df)


def test_add_date_checks_between():
    schema = DataFrameSchema({"date_col": Column(date)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        FORMAT_KEY: "%Y-%m-%d",
        MIN_KEY: "2023-01-01",
        MAX_KEY: "2023-12-31",
    }

    manager._add_date_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "in_range"
    assert (
        check.statistics["min_value"]
        == datetime.strptime("2023-01-01", "%Y-%m-%d").date()
    )
    assert (
        check.statistics["max_value"]
        == datetime.strptime("2023-12-31", "%Y-%m-%d").date()
    )

    df = pd.DataFrame(
        {"date_col": [datetime(2023, 1, 1).date(), datetime(2023, 6, 1).date()]}
    )
    schema.validate(df)


def test_add_date_checks_min_only():
    schema = DataFrameSchema({"date_col": Column(date)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {FORMAT_KEY: "%Y-%m-%d", MIN_KEY: "2023-01-01"}

    manager._add_date_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "greater_than_or_equal_to"
    assert (
        check.statistics["min_value"]
        == datetime.strptime("2023-01-01", "%Y-%m-%d").date()
    )

    df = pd.DataFrame(
        {"date_col": [datetime(2023, 1, 1).date(), datetime(2023, 6, 1).date()]}
    )
    schema.validate(df)


def test_add_date_checks_max_only():
    schema = DataFrameSchema({"date_col": Column(date)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {FORMAT_KEY: "%Y-%m-%d", MAX_KEY: "2023-12-31"}

    manager._add_date_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "less_than_or_equal_to"
    assert (
        check.statistics["max_value"]
        == datetime.strptime("2023-12-31", "%Y-%m-%d").date()
    )

    df = pd.DataFrame(
        {"date_col": [datetime(2023, 1, 1).date(), datetime(2023, 6, 1).date()]}
    )
    schema.validate(df)


def test_add_date_checks_default_format():
    schema = DataFrameSchema({"date_col": Column(date)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {MIN_KEY: "2023-01-01", MAX_KEY: "2023-12-31"}

    manager._add_date_checks("date_col", additional_check)

    assert len(schema.columns["date_col"].checks) == 1
    check = schema.columns["date_col"].checks[0]
    assert check.name == "in_range"
    assert (
        check.statistics["min_value"]
        == datetime.strptime("2023-01-01", DEFAULT_DATE_FORMAT).date()
    )
    assert (
        check.statistics["max_value"]
        == datetime.strptime("2023-12-31", DEFAULT_DATE_FORMAT).date()
    )

    df = pd.DataFrame(
        {
            "date_col": [
                datetime(2023, 1, 1).date(),
                datetime(2023, 6, 1).date(),
                datetime(2023, 12, 22).date(),
            ]
        }
    )
    schema.validate(df)


def test_add_null_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)], nullable=True),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        NULL_COUNT_KEY: 2,
        ROWS_COUNT_KEY: 5,
        MARGIN_ERROR_KEY: 0,
    }

    manager._add_null_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [1.0, None, 2.0, None, 3.0]})
    schema.validate(df)


def test_add_null_checks_with_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)], nullable=True),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {
        NULL_COUNT_KEY: 2,
        ROWS_COUNT_KEY: 5,
        MARGIN_ERROR_KEY: 1,
    }

    manager._add_null_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [1.0, None, 2.0, None, None]})
    schema.validate(df)


def test_add_null_checks_no_null_count():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {ROWS_COUNT_KEY: 5, MARGIN_ERROR_KEY: 0}

    manager._add_null_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [1.0, 2.0, 3.0, 4.0, 5.0]})
    schema.validate(df)


def test_add_null_checks_no_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)], nullable=True),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    additional_check = {NULL_COUNT_KEY: 2, ROWS_COUNT_KEY: 5}

    manager._add_null_checks("col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [1.0, None, 2.0, None, 3.0]})
    schema.validate(df)


def test_proccess_checks_numeric():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "numeric",
                NAME_KEY: "col1",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
                DECIMAL_PRECISION_KEY: 2,
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["col1"].checks) == 1

    df = pd.DataFrame({"col1": [4.50, 5.55, 5.00]})
    result_schema.validate(df)


def test_proccess_checks_boolean():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "boolean",
                NAME_KEY: "col1",
                TRUE_COUNT_KEY: 2,
                FALSE_COUNT_KEY: 1,
                MARGIN_ERROR_KEY: 0,
                ROWS_COUNT_KEY: 3,
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["col1"].checks) == 3

    df = pd.DataFrame({"col1": [True, True, False]})
    result_schema.validate(df)


def test_proccess_checks_date():
    schema = DataFrameSchema({"date_col": Column(date)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "date",
                NAME_KEY: "date_col",
                FORMAT_KEY: "%Y-%m-%d",
                MIN_KEY: "2023-01-01",
                MAX_KEY: "2023-12-31",
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["date_col"].checks) == 1
    check = result_schema.columns["date_col"].checks[0]
    assert check.name == "in_range"
    assert (
        check.statistics["min_value"]
        == datetime.strptime("2023-01-01", "%Y-%m-%d").date()
    )
    assert (
        check.statistics["max_value"]
        == datetime.strptime("2023-12-31", "%Y-%m-%d").date()
    )

    df = pd.DataFrame(
        {"date_col": [datetime(2023, 1, 1).date(), datetime(2023, 6, 1).date()]}
    )
    result_schema.validate(df)


def test_proccess_checks_datetime():
    schema = DataFrameSchema({"datetime_col": Column(datetime)})
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "datetime",
                NAME_KEY: "datetime_col",
                FORMAT_KEY: "%Y-%m-%d %H:%M:%S",
                MIN_KEY: "2023-01-01 00:00:00",
                MAX_KEY: "2023-12-31 23:59:59",
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["datetime_col"].checks) == 1
    check = result_schema.columns["datetime_col"].checks[0]
    assert check.name == "in_range"
    assert check.statistics["min_value"] == datetime(2023, 1, 1, 0, 0, 0)
    assert check.statistics["max_value"] == datetime(2023, 12, 31, 23, 59, 59)

    df = pd.DataFrame(
        {
            "datetime_col": [
                datetime(2023, 1, 1, 0, 0, 0),
                datetime(2023, 6, 1, 12, 0, 0),
            ]
        }
    )
    result_schema.validate(df)


def test_proccess_checks_nullable():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)], nullable=True),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "numeric",
                NAME_KEY: "col1",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
                DECIMAL_PRECISION_KEY: 2,
                NULLABLE_KEY: True,
                NULL_COUNT_KEY: 2,
                ROWS_COUNT_KEY: 5,
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["col1"].checks) == 2

    df = pd.DataFrame({"col1": [4.50, None, 5.55, None, 5.00]})
    result_schema.validate(df)


def test_proccess_checks_column_not_found():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "numeric",
                NAME_KEY: "col2",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
                DECIMAL_PRECISION_KEY: 2,
            }
        ]
    }

    result_schema = manager.proccess_checks(custom_data)

    assert len(result_schema.columns["col1"].checks) == 1  # No new checks added

    df = pd.DataFrame({"col1": [4.50, 5.55, 5.00]})
    result_schema.validate(df)


def test_proccess_checks_invalid_column_name():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                TYPE_KEY: "numeric",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
                DECIMAL_PRECISION_KEY: 2,
            }
        ]
    }

    with pytest.raises(ValueError, match="Column name not defined in the schema"):
        manager.proccess_checks(custom_data)


def test_proccess_checks_invalid_column_type():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )
    manager = PanderaCheckManager("test_checkpoint", schema)
    custom_data = {
        COLUMNS_KEY: [
            {
                NAME_KEY: "col1",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
                DECIMAL_PRECISION_KEY: 2,
            }
        ]
    }

    with pytest.raises(ValueError, match="Type not defined for column col1"):
        manager.proccess_checks(custom_data)
