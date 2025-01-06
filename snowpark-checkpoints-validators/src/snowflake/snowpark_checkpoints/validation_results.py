#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from json import JSONEncoder


def as_validation_result(dct: dict):
    """Convert a dictionary to a ValidationResult object if the dictionary contains a "result" key.

    Args:
        dct (dict): The dictionary to be converted.

    Returns:
        ValidationResult: An instance of ValidationResult if the dictionary contains a "result" key.
        dict: The original dictionary if it does not contain a "result" key.

    """
    if "result" in dct:
        return ValidationResult(**dct)
    return dct


class ValidationResult:

    """A class used to represent the result of a validation.

    Attributes
    ----------
    result : str
        The result of the validation.
    timestamp : str
        The timestamp when the validation was performed.
    file : str
        The file where the validation was performed.
    line_of_code : int
        The specific line of code that was validated.

    Methods
    -------
    __dict__()
        Returns a dictionary representation of the validation result.

    """

    def __init__(
        self,
        result: str,
        timestamp: str,
        file: str,
        line_of_code: int,
    ):
        self.result = result
        self.timestamp = timestamp
        self.file = file
        self.line_of_code = line_of_code

    def __dict__(self) -> dict:
        return {
            "result": self.result,
            "timestamp": self.timestamp,
            "file": self.file,
            "line_of_code": self.line_of_code,
        }

    def __eq__(self, other):
        return self.__dict__() == other.__dict__()


class ValidationResultEncoder(JSONEncoder):

    """ValidationResultEncoder is a custom JSON encoder for serializing ValidationResult objects.

    Methods:
        default(obj):
            Overrides the default method to serialize ValidationResult objects by returning their dictionary
            representation.
            If the object is not an instance of ValidationResult, it falls back to the default serialization method.

    """

    def default(self, obj):
        if isinstance(obj, ValidationResult):
            return obj.__dict__()
        return super().default(obj)
