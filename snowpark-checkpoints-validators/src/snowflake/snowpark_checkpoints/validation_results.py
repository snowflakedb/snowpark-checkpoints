from json import JSONEncoder
from typing import Optional


def as_validation_result(dct):
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

    """A class to represent the result of a validation.

    Attributes:
        result : str
            The result of the validation.
        timestamp : str
            The timestamp when the validation was performed.
        location : int
            The location identifier where the validation was performed.
        file : Optional[str], optional
            The file where the validation was performed (default is None).
        function : Optional[str], optional
            The function where the validation was performed (default is None).

    Methods:
        __dict__() -> dict:
            Returns a dictionary representation of the validation result.

    """

    def __init__(
        self,
        result: str,
        timestamp: str,
        location: int,
        file: Optional[str] = None,
        function: Optional[str] = None,
    ):
        self.result = result
        self.timestamp = timestamp
        self.location = location
        self.file = file
        self.function = function

    def __dict__(self) -> dict:
        return {
            "result": self.result,
            "timestamp": self.timestamp,
            "location": self.location,
            "file": self.file,
            "function": self.function,
        }


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
