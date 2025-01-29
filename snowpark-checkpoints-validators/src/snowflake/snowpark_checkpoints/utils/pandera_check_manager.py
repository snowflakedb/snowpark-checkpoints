from datetime import datetime
from typing import Optional

from pandera import Check, DataFrameSchema

from snowflake.snowpark_checkpoints.utils.checkpoint_logger import CheckpointLogger
from snowflake.snowpark_checkpoints.utils.constants import (
    COLUMNS_KEY,
    DECIMAL_PRECISION_KEY,
    DEFAULT_DATE_FORMAT,
    FALSE_COUNT_KEY,
    FORMAT_KEY,
    MARGIN_ERROR_KEY,
    MAX_KEY,
    MEAN_KEY,
    MIN_KEY,
    NAME_KEY,
    NULL_COUNT_KEY,
    NULLABLE_KEY,
    ROWS_COUNT_KEY,
    SKIP_ALL,
    TRUE_COUNT_KEY,
    TYPE_KEY,
)
from snowflake.snowpark_checkpoints.utils.supported_types import (
    BooleanTypes,
    NumericTypes,
)


class PanderaCheckManager:
    def __init__(self, checkpoint_name: str, schema: DataFrameSchema):
        self.checkpoint_name = checkpoint_name
        self.schema = schema

    def _add_numeric_checks(self, col: str, additional_check: dict[str, any]):
        """Add numeric checks to a specified column in the schema.

        This method adds two types of checks to the specified column:
        1. A mean check that ensures the mean of the column values is within the specified margin of error.
        2. A decimal precision check that ensures the number of decimal places in the column values does not
           exceed the specified precision.

        Args:
            col (str): The name of the column to which the checks will be added.
            additional_check (dict[str, any]): A dictionary containing the following keys:
                - MEAN_KEY: The expected mean value for the column.
                - MARGIN_ERROR_KEY: The acceptable margin of error for the mean check.
                - DECIMAL_PRECISION_KEY: The maximum number of decimal places allowed for the column values.

        """
        mean = additional_check.get(MEAN_KEY, 0)
        std = additional_check.get(MARGIN_ERROR_KEY, 0)

        def check_mean(series):
            series_mean = series.mean()
            return mean - std <= series_mean <= mean + std

        self.schema.columns[col].checks.append(
            Check(check_mean, element_wise=False, name="mean")
        )

        if DECIMAL_PRECISION_KEY in additional_check:
            self.schema.columns[col].checks.append(
                Check(
                    lambda series: series.apply(
                        lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0
                    )
                    <= additional_check[DECIMAL_PRECISION_KEY],
                    name="decimal_precision",
                )
            )

    def _add_boolean_checks(self, col: str, additional_check: dict[str, any]):
        """Add boolean checks to the schema for a specified column.

        This method extends the checks for a given column in the schema by adding
        boolean checks based on the provided additional_check dictionary. It calculates
        the percentage of True and False values in the column and ensures that these
        percentages fall within a specified margin of error.

        Args:
            col (str): The name of the column to which the checks will be added.
            additional_check (dict[str, any]): A dictionary containing the following keys:
                - TRUE_COUNT_KEY: The count of True values in the column.
                - FALSE_COUNT_KEY: The count of False values in the column.
                - ROWS_COUNT_KEY: The total number of rows in the column.
                - MARGIN_ERROR_KEY: The acceptable margin of error for the percentage checks.

        Returns:
            None

        """
        count_of_true = additional_check.get(TRUE_COUNT_KEY, 0)
        count_of_false = additional_check.get(FALSE_COUNT_KEY, 0)
        rows_count = additional_check.get(ROWS_COUNT_KEY, 0)
        std = additional_check.get(MARGIN_ERROR_KEY, 0)
        percentage_true = count_of_true / rows_count
        percentage_false = count_of_false / rows_count

        self.schema.columns[col].checks.extend(
            [
                Check(
                    lambda series: (
                        percentage_true - std
                        <= series.value_counts().get(True, 0) / series.count()
                        if series.count() > 0
                        else 1 <= percentage_true + std
                    ),
                ),
                Check(
                    lambda series: (
                        percentage_false - std
                        <= series.value_counts().get(False, 0) / series.count()
                        if series.count() > 0
                        else 1 <= percentage_false + std
                    ),
                ),
            ]
        )

    def _add_null_checks(self, col: str, additional_check: dict[str, any]):
        """Add null checks to the schema for a specified column.

        This method calculates the percentage of null values in the column and
        appends a check to the schema that ensures the percentage of null values
        in the series is within an acceptable margin of error.

        Args:
            col (str): The name of the column to add null checks for.
            additional_check (dict[str, any]): A dictionary containing additional
                check parameters:
                - NULL_COUNT_KEY (str): The key for the count of null values.
                - ROWS_COUNT_KEY (str): The key for the total number of rows.
                - MARGIN_ERROR_KEY (str): The key for the margin of error.

        Raises:
            KeyError: If any of the required keys are missing from additional_check.

        """
        count_of_null = additional_check.get(NULL_COUNT_KEY, 0)
        rows_count = additional_check.get(ROWS_COUNT_KEY, 0)
        std = additional_check.get(MARGIN_ERROR_KEY, 0)
        percentage_null = count_of_null / rows_count

        self.schema.columns[col].checks.append(
            Check(
                lambda series: (
                    percentage_null - std <= series.isnull().sum() / series.count()
                    if series.count() > 0
                    else 1 <= percentage_null + std
                ),
            ),
        )

    def _add_date_time_checks(self, col: str, additional_check: dict[str, any]):
        """Add date and time checks to a specified column in the given DataFrameSchema.

        Args:
            schema (DataFrameSchema): The schema to which the checks will be added.
            col (str): The name of the column to which the checks will be applied.
            additional_check (dict[str, Any]): A dictionary containing additional check parameters.
                - FORMAT_KEY (str): The key for the date format string in the dictionary.
                - MIN_KEY (str): The key for the minimum date value in the dictionary.
                - MAX_KEY (str): The key for the maximum date value in the dictionary.

        The function will add the following checks based on the provided additional_check dictionary:
        - If both min_date and max_date are provided, a between check is added.
        - If only min_date is provided, a greater_than_or_equal_to check is added.
        - If only max_date is provided, a less_than_or_equal_to check is added.

        """
        format = additional_check.get(FORMAT_KEY, DEFAULT_DATE_FORMAT)

        min = additional_check.get(MIN_KEY, None)
        min_date = datetime.strptime(min, format) if min else None

        max = additional_check.get(MAX_KEY, None)
        max_date = datetime.strptime(max, format) if max else None

        if min_date and max_date:
            self.schema.columns[col].checks.append(
                Check.between(
                    min_date,
                    max_date,
                    include_max=True,
                    include_min=True,
                )
            )
        elif min_date:
            self.schema.columns[col].checks.append(
                Check.greater_than_or_equal_to(min_date)
            )
        elif max_date:
            self.schema.columns[col].checks.append(
                Check.less_than_or_equal_to(max_date)
            )

    def _add_date_checks(self, col: str, additional_check: dict[str, any]):
        """Add date and time checks to a specified column in the given DataFrameSchema.

        Args:
            schema (DataFrameSchema): The schema to which the checks will be added.
            col (str): The name of the column to which the checks will be applied.
            additional_check (dict[str, Any]): A dictionary containing additional check parameters.
                - FORMAT_KEY (str): The key for the date format string in the dictionary.
                - MIN_KEY (str): The key for the minimum date value in the dictionary.
                - MAX_KEY (str): The key for the maximum date value in the dictionary.

        The function will add the following checks based on the provided additional_check dictionary:
        - If both min_date and max_date are provided, a between check is added.
        - If only min_date is provided, a greater_than_or_equal_to check is added.
        - If only max_date is provided, a less_than_or_equal_to check is added.

        """
        format = additional_check.get(FORMAT_KEY, DEFAULT_DATE_FORMAT)

        min = additional_check.get(MIN_KEY, None)
        min_date = datetime.strptime(min, format).date() if min else None

        max = additional_check.get(MAX_KEY, None)
        max_date = datetime.strptime(max, format).date() if max else None

        if min_date and max_date:
            self.schema.columns[col].checks.append(
                Check.between(
                    min_date,
                    max_date,
                    include_max=True,
                    include_min=True,
                )
            )
        elif min_date:
            self.schema.columns[col].checks.append(
                Check.greater_than_or_equal_to(min_date)
            )
        elif max_date:
            self.schema.columns[col].checks.append(
                Check.less_than_or_equal_to(max_date)
            )

    def proccess_checks(self, custom_data: dict) -> DataFrameSchema:
        """Process the checks defined in the custom_data dictionary and applies them to the schema.

        Args:
            custom_data (dict): A dictionary containing the custom checks to be applied. The dictionary
                                should have a key corresponding to COLUMNS_KEY, which maps to a list of
                                column check definitions. Each column check definition should include
                                the following keys:
                                - TYPE_KEY: The type of the column (e.g., numeric, boolean, date, datetime).
                                - NAME_KEY: The name of the column.
                                - NULLABLE_KEY: A boolean indicating if the column is nullable.

        Returns:
            DataFrameSchema: The updated schema with the applied checks.

        Raises:
            ValueError: If the column name or type is not defined in the schema.

        """
        logger = CheckpointLogger().get_logger()
        for additional_check in custom_data.get(COLUMNS_KEY):

            type = additional_check.get(TYPE_KEY, None)
            name = additional_check.get(NAME_KEY, None)
            is_nullable = additional_check.get(NULLABLE_KEY, False)

            if name is None:
                raise ValueError(
                    f"Column name not defined in the schema {self.checkpoint_name}"
                )

            if type is None:
                raise ValueError(f"Type not defined for column {name}")

            if self.schema.columns.get(name) is None:
                logger.warning(f"Column {name} not found in schema")
                continue

            if type in NumericTypes:
                self._add_numeric_checks(name, additional_check)

            elif type in BooleanTypes:
                self._add_boolean_checks(name, additional_check)

            elif type == "date":
                self._add_date_checks(name, additional_check)

            elif type == "datetime":
                self._add_date_time_checks(name, additional_check)

            if is_nullable:
                self._add_null_checks(name, additional_check)

        return self.schema

    def skip_checks_on_schema(
        self,
        skip_checks: Optional[dict[str, list[str]]] = None,
    ) -> DataFrameSchema:
        """Modify the schema by skipping specified checks on columns.

        Args:
            skip_checks : Optional[dict[str, list[str]]], optional
                A dictionary where keys are column names and values are lists of check names to skip.
                If the special key 'SKIP_ALL' is present in the list of checks for a column, all checks
                for that column will be skipped. If None, no checks will be skipped.

        Returns:
            DataFrameSchema: The modified schema with specified checks skipped.

        """
        if not skip_checks:
            return self.schema

        for col, checks_to_skip in skip_checks.items():

            if col in self.schema.columns:

                if SKIP_ALL in checks_to_skip:
                    self.schema.columns[col].checks = {}
                else:
                    self.schema.columns[col].checks = [
                        check
                        for check in self.schema.columns[col].checks
                        if check.name not in checks_to_skip
                    ]

        return self.schema

    def add_custom_checks(
        self,
        custom_checks: Optional[dict[str, list[Check]]] = None,
    ):
        """Add custom checks to a Pandera DataFrameSchema.

        Args:
            schema (DataFrameSchema): The Pandera DataFrameSchema object to modify.
            custom_checks (Optional[dict[str, list[Check]]]): A dictionary where keys are column names
                                                    and values are lists of checks to add for
                                                    those columns.

        Returns:
            None

        """
        if not custom_checks:
            return self.schema

        for col, checks in custom_checks.items():

            if col in self.schema.columns:
                col_schema = self.schema.columns[col]
                col_schema.checks.extend(checks)
            else:
                raise ValueError(f"Column {col} not found in schema")

        return self.schema
