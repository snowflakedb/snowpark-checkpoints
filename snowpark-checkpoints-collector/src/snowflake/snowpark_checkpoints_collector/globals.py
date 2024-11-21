import locale

# SCHEMA CONTRACT KEYS CONSTANTS
COLUMN_TYPE_KEY = "type"
COLUMN_COUNT_KEY = "rows_count"
COLUMN_ROWS_NOT_NULL_COUNT_KEY = "rows_not_null_count"
COLUMN_NULL_COUNT_KEY= "rows_null_count"
COLUMN_MEAN_KEY = "mean"
COLUMN_MARGIN_ERROR_KEY = "margin_error"
COLUMN_MAX_KEY = "max"
COLUMN_MIN_KEY = "min"
COLUMN_DECIMAL_PRECISION_KEY = "decimal_precision"
COLUMN_TRUE_COUNT_KEY = "true_count"
COLUMN_FALSE_COUNT_KEY = "false_count"

DATAFRAME_PANDERA_SCHEMA_KEY = "pandera_schema"
DATAFRAME_CUSTOM_DATA_KEY = "custom_data"

# FORMAT CONSTANTS
CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT = "snowpark-{}-schema-contract.json"
BETWEEN_CHECK_ERROR_MESSAGE_FORMAT = 'Value must be between {} and {}'

# MISC KEYS
DECIMAL_TOKEN_KEY = "decimal_point"

def get_decimal_token():
    decimal_token = locale.localeconv()[DECIMAL_TOKEN_KEY]
    return decimal_token