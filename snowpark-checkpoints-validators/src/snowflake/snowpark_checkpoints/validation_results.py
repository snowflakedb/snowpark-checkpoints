# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
