import json
import sys
import os
import argparse


class CoverageBelowThresholdError(Exception):
    """Custom exception for coverage below threshold"""

    pass


def validate_coverage(coverage_file, coverage_threshold):
    """
    Read and validate the coverage JSON file.

    Args:
        coverage_file (str): Path to the coverage JSON file
        coverage_threshold (float): Minimum acceptable coverage percentage

    Raises:
        CoverageBelowThresholdError: If coverage is below the specified threshold
    """
    if not os.path.exists(coverage_file):
        print(f"Error: Coverage file not found - {coverage_file}")
        sys.exit(1)

    try:
        with open(coverage_file) as f:
            coverage_data = json.load(f)

        totals = coverage_data.get("totals", {})

        print("Coverage Report:")
        print(f"Total Covered Lines: {totals.get('covered_lines', 0)}")
        print(f"Total Statements: {totals.get('num_statements', 0)}")
        print(f"Percent Covered: {totals.get('percent_covered_display', '0')}%")
        print(f"Missing Lines: {totals.get('missing_lines', 0)}")

        current_coverage = totals.get("percent_covered", 0)

        try:
            coverage_threshold = float(coverage_threshold)
        except (ValueError, TypeError):
            print(
                f"Error: Invalid threshold value '{coverage_threshold}'. Must be a number."
            )
            sys.exit(1)

        if current_coverage < coverage_threshold:
            error_message = (
                f"FAIL: Coverage {current_coverage:.2f}% is below the required threshold of {coverage_threshold}%\n"
                f"Action Required: Improve test coverage to meet or exceed {coverage_threshold}%"
            )
            raise CoverageBelowThresholdError(error_message)

        print(
            f"\nPASS: Coverage {current_coverage:.2f}% meets the threshold requirement of {coverage_threshold}%"
        )
        return totals

    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in coverage file - {coverage_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error reading coverage file: {e}")
        sys.exit(1)


def main():
    """
    Main function to parse arguments and execute the script.
    """
    parser = argparse.ArgumentParser(description="Validate test coverage")
    parser.add_argument("coverage_file", help="Path to the coverage JSON file")
    parser.add_argument(
        "--threshold",
        default=os.environ.get("COVERAGE_THRESHOLD", "95"),
        help="Coverage threshold percentage (default: 95 or from COVERAGE_THRESHOLD env)",
    )
    args = parser.parse_args()

    try:
        validate_coverage(args.coverage_file, args.threshold)
        sys.exit(0)
    except CoverageBelowThresholdError as e:
        print(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
