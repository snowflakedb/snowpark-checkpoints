# snowpark-checkpoints-hypothesis

**snowpark-checkpoints-hypothesis** is a [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) extension for generating Snowpark DataFrames. This project provides strategies to facilitate testing and data generation for Snowpark DataFrames using the Hypothesis library.

## Usage

To generate a Snowpark DataFrame using Hypothesis, you need to pass the `dataframe_strategy` function to the Hypothesis `@given` decorator as shown below:

```python
from hypothesis import given
from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session


@given(
    df=dataframe_strategy(
        schema="schema.json",
        session=Session.builder.getOrCreate(),
        size=10
    )
)
def test_my_function(df: DataFrame):
    # Test your function here
    ...
```

## Development

### Set up a development environment

To set up a development environment, follow the steps below:

1. Create a virtual environment using **venv** or **conda**. Replace \<env-name\> with the name of your environment.

    Using **venv**:

    ```shell
    python3.11 -m venv <env-name>
    source <env-name>/bin/activate
    ```

    Using **conda**:

    ```shell
    conda create -n <env-name> python=3.11
    conda activate <env-name>
    ```

2. Configure your IDE to use the previously created virtual environment:

    * [Configuring a Python interpreter in PyCharm](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html)
    * [Configuring a Python interpreter in VS Code](https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter)

3. Install the project dependencies:

    ```shell
    pip install hatch
    pip install -e .
    ```

### Running Tests

To run tests, run the following command.

```shell
hatch run test:check
```
