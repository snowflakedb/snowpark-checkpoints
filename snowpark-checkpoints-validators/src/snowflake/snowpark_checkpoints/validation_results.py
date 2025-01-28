#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


from pydantic import BaseModel


class ValidationResult(BaseModel):

    """ValidationResult represents the result of a validation checkpoint.

    Attributes:
        result (str): The result of the validation.
        timestamp (datetime): The timestamp when the validation was performed.
        file (str): The file where the validation checkpoint is located.
        line_of_code (int): The line number in the file where the validation checkpoint is located.
        checkpoint_name (str): The name of the validation checkpoint.

    """

    result: str
    timestamp: str
    file: str
    line_of_code: int
    checkpoint_name: str


class ValidationResults(BaseModel):

    """ValidationResults is a model that holds a list of validation results.

    Attributes:
        results (list[ValidationResult]): A list of validation results.

    """

    results: list[ValidationResult]
