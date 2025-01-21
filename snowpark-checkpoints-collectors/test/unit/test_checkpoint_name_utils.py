import pytest

from snowflake.snowpark_checkpoints_collector.utils import checkpoint_name_utils


@pytest.mark.parametrize(
    "input_value,expected_value",
    [("my checkpoint", "my_checkpoint"), ("my  checkpoint ", "my__checkpoint_")],
)
def test_normalize_checkpoint_name_whitespace_case(input_value, expected_value):
    normalized_checkpoint_name = checkpoint_name_utils.normalize_checkpoint_name(
        input_value
    )
    assert normalized_checkpoint_name == expected_value


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        ("my-checkpoint", "my_checkpoint"),
    ],
)
def test_normalize_checkpoint_name_hyphen_case(input_value, expected_value):
    normalized_checkpoint_name = checkpoint_name_utils.normalize_checkpoint_name(
        input_value
    )
    assert normalized_checkpoint_name == expected_value


@pytest.mark.parametrize(
    "input_value", ["_checkpoint1", "_checkpoint", "checkPoint1", "Checkpoint", "_1"]
)
def test_validate_checkpoint_name_valid_case(input_value):
    is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
        input_value
    )
    assert is_valid_checkpoint_name is True


@pytest.mark.parametrize(
    "input_value", ["_", "5", "", "56_my_checkpoint", "my-check", "_+check"]
)
def test_validate_checkpoint_name_invalid_case(input_value):
    is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
        input_value
    )
    assert is_valid_checkpoint_name is False
