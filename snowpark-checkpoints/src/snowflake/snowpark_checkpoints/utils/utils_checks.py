from typing import Any, Dict
from pandera import DataFrameSchema
import pandera as pa


def add_numeric_checks(
    schema: DataFrameSchema, col: str, additional_check: Dict[str, Any]
):
    mean = additional_check["mean"]
    std = additional_check["std"]

    def check_mean(series):
        series_mean = series.mean()
        return mean - std <= series_mean <= mean + std

    schema.columns[col].checks.append(pa.Check(check_mean, element_wise=True))


def add_boolean_checks(
    schema: DataFrameSchema, col: str, additional_check: Dict[str, Any]
):
    count_of_true = additional_check["count_of_true"]
    count_of_false = additional_check["count_of_false"]
    std = additional_check["std"]

    schema.columns[col].checks.extend(
        [
            pa.Check(
                lambda series: count_of_true - std
                <= series.value_counts()[True]
                <= count_of_true + std
            ),
            pa.Check(
                lambda series: count_of_false - std
                <= series.value_counts()[False]
                <= count_of_false + std
            ),
        ]
    )
