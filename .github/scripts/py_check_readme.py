#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from datetime import datetime


def get_file_date(file_path):
    timestamp = os.path.getmtime(file_path)
    return datetime.fromtimestamp(timestamp).date()


def get_file_datetime(file_path):
    timestamp = os.path.getmtime(file_path)
    return datetime.fromtimestamp(timestamp)


def get_file_datetime_hour(file_path):
    timestamp = os.path.getmtime(file_path)
    dt = datetime.fromtimestamp(timestamp)
    return dt.replace(minute=0, second=0, microsecond=0)


def check_readme_changes():
    root_readme = Path("README.md")
    if not root_readme.exists():
        print("Error: Root README.md does not exist")
        sys.exit(1)

    root_readme_datetime_hour = get_file_datetime_hour(root_readme)
    has_subdir_changes = False
    changed_readmes = []

    for root, dirs, files in os.walk("."):
        if root == ".":
            continue

        if (
            "dist" in root
            or ".git" in root
            or "/docs" in root
            or "/.github" in root
            or "/snowpark-checkpoints-testing" in root
            or "/.pytest_cache" in root
            or "/.hatch/" in root
        ):
            continue
        if "README.md" in files:
            subdir_readme = Path(root) / "README.md"
            subdir_datetime_hour = get_file_datetime_hour(subdir_readme)
            if subdir_datetime_hour > root_readme_datetime_hour:
                has_subdir_changes = True
                changed_readmes.append(str(subdir_readme))

    if has_subdir_changes:
        print(
            "WARNING: Some of these files need to be synchronized with the root README.md. "
            "Please review this before proceeding with the commit."
        )
        for readme in changed_readmes:
            print(f" - {readme}")
        sys.exit(1)
    else:
        print("OK: No subdirectory README.md files found newer than root README.md")
        sys.exit(0)


if __name__ == "__main__":
    check_readme_changes()
