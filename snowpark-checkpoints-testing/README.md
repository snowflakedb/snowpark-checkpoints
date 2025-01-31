## snowpark-checkpoints-testing

### E2E Tests

The E2E tests are designed to validate the complete functionality of the Snowpark Checkpoints library by simulating real-world scenarios. These tests ensure that all components of the library work together as expected.

### Stress Tests

The stress tests evaluate the performance and stability of the Snowpark Checkpoints library under heavy load. These tests help identify potential bottlenecks and erroneous behaviors

### Directory Structure

- `test/e2e/`: Contains E2E tests.
- `test/stress/mode_dataframe/`: Contains stress tests for DataFrame mode.
- `test/stress/mode_schema/`: Contains stress tests for Schema mode.

### Running Tests

Follow the steps below to set up the environment and run the tests.

#### Requirements

- Python >= 3.9
- OpenJDK 21.0.2
- pytest >= 8.3.3
- psutil >= 6.1.1
- snowpark-checkpoints >= 0.1.0rc2
- snowpark-checkpoints-collectors >= 0.1.0rc2
- snowpark-checkpoints-validators >= 0.1.0rc2
- Snow CLI: The default connection needs to have a database and a schema.

#### Steps

1. Create a Python environment with Python 3.9 or higher in the root dir.
2. Install the snowpark-checkpoints-collector package

```bash
pip install -e ./snowpark-checkpoints-collector
```

3. Install the snowpark-checkpoints-validator package

```bash
pip install -e ./snowpark-checkpoints-validator
```

4. Install the development dependencies from ./snowpark-checkpoints-testing

```bash
python -m pip install -e ".[development]"
```

5. Run E2E tests from ./snowpark-checkpoints-testing directory

```bash
pytest test/e2e/test_e2e_checkpoints.py
```

6. Run stress tests from ./snowpark-checkpoints-testing directory

```bash
pytest test/stress/mode_dataframe/test_performance_mode_dataframe.py
```

```bash
pytest test/stress/mode_schema/test_performance_mode_schema.py
```
