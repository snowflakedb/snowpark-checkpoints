# /// script
# dependencies = [
#   'tomli; python_version < "3.11"',
# ]
# ///

import argparse
import operator
import pathlib
import sys
import textwrap
import typing

from typing import Tuple


if sys.version_info < (3, 11):
    from tomli import load
else:
    from tomllib import load

LIBS_DIR = pathlib.Path(__file__).absolute().parent.parent / "hypothesis-snowpark"
NL = "\n"


def get_lib_paths(lib_name: str) -> Tuple[pathlib.Path, pathlib.Path]:
    """Get paths for the library directory and requirements file."""
    lib_dir = LIBS_DIR / lib_name
    req_file = lib_dir / "requirements.txt"
    return lib_dir, req_file


def create_req_file(lib_name: str) -> int:
    lib_dir, req_file = get_lib_paths(lib_name)

    # Check if library exists
    if not lib_dir.exists():
        print(f"Error: Library directory '{lib_name}' not found in {LIBS_DIR}")
        return 1

    # Read pyproject.toml
    pyproject_path = lib_dir / "pyproject.toml"
    if not pyproject_path.exists():
        print(f"Error: pyproject.toml not found in {lib_dir}")
        return 1

    with open(pyproject_path, "rb") as fp:
        pyproject_content = load(fp)

    try:
        deps: typing.List[str] = pyproject_content["project"]["dependencies"]
    except KeyError:
        print(f"Error: No dependencies found in {pyproject_path}")
        return 1

    deps_set = set(deps)
    deps_str = NL.join(deps)

    # Check existing requirements if file exists
    if req_file.exists():
        with open(req_file) as f:
            existing_reqs_set = set(
                map(operator.methodcaller("strip"), f.read().splitlines())
            )

        should_redump = deps_set != existing_reqs_set
        if not should_redump:
            print(f"Requirements haven't changed for {lib_name}")
            return 0

    print(
        f"requirements.txt contents for {lib_name}:\n" + textwrap.indent(deps_str, "  ")
    )
    with open(req_file, "w") as f:
        f.write(deps_str)
    print(f"requirements.txt written for {lib_name}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Create requirements.txt from pyproject.toml dependencies"
    )
    parser.add_argument("lib_name", help="Name of the library in libs directory")

    args = parser.parse_args()
    return create_req_file(args.lib_name)


if __name__ == "__main__":
    sys.exit(main())
