#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime

import hypothesis.strategies as st
import pandas as pd
import pandera as pa
import pytest

from snowflake.hypothesis_snowpark.checks.date_check import (
    dates_in_range,  # noqa: F401 Import required to register the custom checks
)


def test_date_strategy_chained_strategies():
    check = pa.Check.dates_in_range(
        min_value=datetime.date(2020, 1, 1),
        max_value=datetime.date(2024, 12, 31),
        include_min=True,
        include_max=True,
    )

    with pytest.raises(
        ValueError, match="Chaining strategies is not supported for 'date' dtype."
    ):
        check.strategy(pa.Date, **check.statistics, strategy=st.dates())


def test_date_strategy_invalid_range():
    check = pa.Check.dates_in_range(
        min_value=datetime.date(2024, 12, 31),
        max_value=datetime.date(2020, 1, 1),
        include_min=True,
        include_max=True,
    )

    with pytest.raises(
        ValueError,
        match="Invalid range: min_value must be less than or equal to max_value.",
    ):
        check.strategy(pa.Date, **check.statistics)


@pytest.mark.parametrize(
    "include_min, include_max",
    [(False, True), (True, False), (False, False), (True, True)],
)
def test_date_strategy_min_max(include_min: bool, include_max: bool):
    min_value = datetime.date(2020, 1, 1)
    max_value = datetime.date(2020, 12, 31)

    check = pa.Check.dates_in_range(
        min_value=min_value,
        max_value=max_value,
        include_min=include_min,
        include_max=include_max,
    )

    strategy = check.strategy(pa.Date, **check.statistics)
    result = strategy.example()

    assert isinstance(result, datetime.date)
    assert (result >= min_value if include_min else result > min_value) & (
        result <= max_value if include_max else result < max_value
    )


def test_dates_in_range_valid_range():
    df = pd.DataFrame(
        {
            "c1": [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 6, 1),
                datetime.date(2020, 12, 31),
            ]
        }
    )

    schema = pa.DataFrameSchema(
        columns={
            "c1": pa.Column(
                pa.Date,
                checks=pa.Check.dates_in_range(
                    min_value=datetime.date(2020, 1, 1),
                    max_value=datetime.date(2020, 12, 31),
                    include_min=True,
                    include_max=True,
                ),
            )
        }
    )

    result = schema.validate(df)
    assert isinstance(result, pd.DataFrame)


def test_dates_in_range_invalid_range():
    df = pd.DataFrame(
        {
            "c1": [
                datetime.date(2020, 1, 1),
                datetime.date(2020, 6, 1),
                datetime.date(2020, 12, 30),
            ]
        }
    )

    schema = pa.DataFrameSchema(
        columns={
            "c1": pa.Column(
                pa.Date,
                checks=pa.Check.dates_in_range(
                    min_value=datetime.date(2020, 1, 1),
                    max_value=datetime.date(2020, 12, 31),
                    include_min=False,
                    include_max=False,
                ),
            )
        }
    )

    with pytest.raises(pa.errors.SchemaError):
        schema.validate(df)
