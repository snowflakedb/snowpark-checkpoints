import toml
import os
import argparse


def find_pyproject_toml(directory):
    """
    Recursively searches for a file named 'pyproject.toml' in the given directory.
    """
    for root, _, files in os.walk(directory):
        if "pyproject.toml" in files:
            return os.path.join(root, "pyproject.toml")
    return None


def create_requirements_from_pyproject(pyproject_path):
    """
    Reads the pyproject.toml file and generates a requirements.txt file in the same directory.
    """
    try:
        if not os.path.exists(pyproject_path):
            raise FileNotFoundError(f"The file {pyproject_path} does not exist.")

        with open(pyproject_path, encoding="utf-8") as file:
            pyproject_data = toml.load(file)

        requirements = []

        if "dependencies" in pyproject_data.get("project", {}):
            requirements.extend(pyproject_data["project"]["dependencies"])

        optional_dependencies = pyproject_data.get("project", {}).get(
            "optional-dependencies", {}
        )
        for deps in optional_dependencies.values():
            requirements.extend(deps)

        requirements = sorted(set(requirements))

        output_file = os.path.join(os.path.dirname(pyproject_path), "requirements.txt")
        with open(output_file, "w", encoding="utf-8") as file:
            file.write("\n".join(requirements))

        print(
            f"File {output_file} successfully generated with {len(requirements)} dependencies."
        )
        return output_file
    except Exception as e:
        print(f"An error occurred while processing {pyproject_path}: {e}")
        return None


def display_requirements_file(file_path):
    """
    Displays the contents of the new requirements.txt file.
    """
    try:
        print(f"\nContents of new file generated {file_path}:\n")
        with open(file_path, encoding="utf-8") as file:
            print(file.read())
    except Exception as e:
        print(f"An error occurred while reading this file {file_path}: {e}")


def main():
    """
    Main function to parse arguments and execute the script.
    """
    parser = argparse.ArgumentParser(
        description="Generate a requirements.txt from a pyproject.toml file."
    )
    parser.add_argument(
        "--directory",
        help="Path to the directory where the script will search for a pyproject.toml file recursively.",
    )
    args = parser.parse_args()

    pyproject_path = find_pyproject_toml(args.directory)
    if pyproject_path:
        print(f"Found pyproject.toml file: {pyproject_path}")
        requirements_file = create_requirements_from_pyproject(pyproject_path)
        if requirements_file:
            display_requirements_file(requirements_file)
    else:
        print(f"No pyproject.toml file found in {args.directory}.")


if __name__ == "__main__":
    main()
