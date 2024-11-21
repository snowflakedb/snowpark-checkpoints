from .globals import *
from pandera import Check

def add_numeric_type_checks(pandas_df, pandera_column, column):
    column_values = pandas_df[column]
    min_value = column_values.min().item()
    max_value = column_values.max().item()
    pandera_column.checks.append(
        Check.between(
            min_value=min_value,
            max_value=max_value,
            include_max=True,
            include_min=True,
            title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value)
        )
    )

def get_numeric_type_custom_data(pandas_df, column, column_type):
    column_values = pandas_df[column]
    column_size = len(column_values)
    rows_not_null_count = column_values.count().item()
    rows_null_count = column_size - rows_not_null_count
    min_value = column_values.min().item()
    max_value = column_values.max().item()
    mean_value = column_values.mean().item()
    decimal_precision = compute_decimal_precision(column_values)
    margin_error = column_values.std().item()

    custom_data = {COLUMN_TYPE_KEY: column_type,
                   COLUMN_COUNT_KEY: column_size,
                   COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
                   COLUMN_NULL_COUNT_KEY: rows_null_count,
                   COLUMN_MIN_KEY: min_value,
                   COLUMN_MAX_KEY: max_value,
                   COLUMN_MEAN_KEY: mean_value,
                   COLUMN_DECIMAL_PRECISION_KEY: decimal_precision,
                   COLUMN_MARGIN_ERROR_KEY: margin_error}

    return custom_data

def compute_decimal_precision(column_values):
    decimal_split_size = 2
    decimal_part_index = 1
    decimal_token = get_decimal_token()
    max_decimal_digits_counted = 0
    for value in column_values:
        value_str = str(value)
        value_split_by_token = value_str.split(decimal_token)
        has_decimal_part = len(value_split_by_token) == decimal_split_size
        if not has_decimal_part:
            continue

        decimal_part = value_split_by_token[decimal_part_index]
        decimal_digits_counted = len(decimal_part)
        max_decimal_digits_counted = max(decimal_digits_counted, max_decimal_digits_counted)

    return max_decimal_digits_counted

def add_string_type_checks(pandas_df, pandera_column, column):
    pass

def get_string_type_custom_data(pandas_df, column, column_type):
    column_values = pandas_df[column]
    column_size = len(column_values)
    rows_not_null_count = column_values.count().item()
    rows_null_count = column_size - rows_not_null_count

    custom_data = {COLUMN_TYPE_KEY: column_type,
                   COLUMN_COUNT_KEY: column_size,
                   COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
                   COLUMN_NULL_COUNT_KEY: rows_null_count}

    return custom_data

def add_timestamp_type_checks(pandas_df, pandera_column, column):
    column_values = pandas_df[column]
    min_value = str(column_values.min())
    max_value = str(column_values.max())
    pandera_column.checks.append(
        Check.between(
            min_value=min_value,
            max_value=max_value,
            include_max=True,
            include_min=True,
            title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value)
        )
    )

def get_timestamp_type_custom_data(pandas_df, column, column_type):
    column_values = pandas_df[column]
    column_size = len(column_values)
    rows_not_null_count = column_values.count().item()
    rows_null_count = column_size - rows_not_null_count
    min_value = str(column_values.min())
    max_value = str(column_values.max())

    custom_data = {COLUMN_TYPE_KEY: column_type,
                   COLUMN_COUNT_KEY: column_size,
                   COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
                   COLUMN_NULL_COUNT_KEY: rows_null_count,
                   COLUMN_MIN_KEY: min_value,
                   COLUMN_MAX_KEY: max_value}

    return custom_data

def get_daytimeinterval_type_custom_data(pandas_df, column, column_type):
    column_values = pandas_df[column]
    column_size = len(column_values)
    rows_not_null_count = column_values.count().item()
    rows_null_count = column_size - rows_not_null_count
    min_value = str(column_values.min())
    max_value = str(column_values.max())

    custom_data = {COLUMN_TYPE_KEY: column_type,
                   COLUMN_COUNT_KEY: column_size,
                   COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
                   COLUMN_NULL_COUNT_KEY: rows_null_count,
                   COLUMN_MIN_KEY: min_value,
                   COLUMN_MAX_KEY: max_value}

    return custom_data

def add_daytimeinterval_type_check(pandas_df, pandera_column, column):
    column_values = pandas_df[column]
    min_value = str(column_values.min())
    max_value = str(column_values.max())
    pandera_column.checks.append(
        Check.between(
            min_value=min_value,
            max_value=max_value,
            include_max=True,
            include_min=True,
            title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value)
        )
    )

def add_boolean_type_check(pandera_column):
    pandera_column.checks.extend([Check.isin([True, False])])

def get_boolean_type_custom_data(pandas_df, column, column_type):
    column_values = pandas_df[column]
    column_size = len(column_values)
    rows_not_null_count = column_values.count().item()
    rows_null_count = column_size - rows_not_null_count
    true_count = column_values[column_values == True].count().item()
    false_count = column_values[column_values == False].count().item()

    custom_data = {COLUMN_TYPE_KEY: column_type,
                   COLUMN_COUNT_KEY: column_size,
                   COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
                   COLUMN_NULL_COUNT_KEY: rows_null_count,
                   COLUMN_TRUE_COUNT_KEY: true_count,
                   COLUMN_FALSE_COUNT_KEY: false_count}

    return custom_data