from .column_collection_utils import *
from pyspark.sql import DataFrame as SparkDataFrame
import pandera as pa
import json

def collect_pandera_input_schema(df:SparkDataFrame):
    pass

def collect_pandera_output_schema(df:SparkDataFrame):
    pass

def collect_dataframe_schema_contract(df:SparkDataFrame,
                                      checkpoint_name,
                                      sample=1.0):

    try:
        column_type_dict = get_spark_column_types(df)
        source_df = df.sample(sample)
        pandas_df = source_df.toPandas()
        pandera_infer_schema = pa.infer_schema(pandas_df)
        dataframe_custom_column_data = {}

        for column in pandera_infer_schema.columns:
            column_type = column_type_dict[column]
            pandera_column = pandera_infer_schema.columns[column]
            match column_type:

                case 'long':
                    add_numeric_type_checks(pandas_df, pandera_column, column)
                    custom_data = get_numeric_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case 'double':
                    add_numeric_type_checks(pandas_df, pandera_column, column)
                    custom_data = get_numeric_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case 'string':
                    add_string_type_checks(pandas_df, pandera_column, column)
                    custom_data = get_string_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case 'timestamp':
                    add_timestamp_type_checks(pandas_df, pandera_column, column)
                    custom_data = get_timestamp_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case 'daytimeinterval':
                    add_daytimeinterval_type_check(pandas_df, pandera_column, column)
                    custom_data = get_daytimeinterval_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case 'boolean':
                    add_boolean_type_check(pandera_column)
                    custom_data = get_boolean_type_custom_data(pandas_df, column, column_type)
                    dataframe_custom_column_data[column] = custom_data

                case default:
                    pass

        elif pandas.api.types.is_bool_dtype(col_dtype):
            schema.columns[col].checks.extend([Check.isin([True, False])])

        elif pandas.api.types.is_numeric_dtype(col_dtype):
            min_value = convert_string_to_number(describe_df.loc["min"][col])
            max_value = convert_string_to_number(describe_df.loc["max"][col])

        dataframe_schema_contract_json = json.dumps(dataframe_schema_contract)

        generate_json_checkpoint_file(checkpoint_name, dataframe_schema_contract_json)

    except Exception as err:
        print(err)

def get_spark_column_types(df:SparkDataFrame):
    schema = df.schema
    column_type_collection = {}
    for field in schema.fields:
        column_name = field.name
        type_name = field.dataType.typeName()
        column_type_collection[column_name] = type_name
    return column_type_collection

def generate_json_checkpoint_file(checkpoint_name, dataframe_schema_contract):
    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(checkpoint_name)
    f = open(checkpoint_file_name, "w")
    f.write(dataframe_schema_contract)