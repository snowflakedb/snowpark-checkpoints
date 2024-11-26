import re
import subprocess
import argparse
import pkg_resources


def get_installed_snowflake_cli_version():
    """Retrieve the installed Snowflake CLI version using snow --info."""
    try:

        result = subprocess.run(
            ["pip", "list"], capture_output=True, text=True, check=True
        )
        version_match = re.search(r"snowflake-cli\s+(\d+\.\d+\.\d+)", result.stdout)

        if version_match:
            return version_match.group(1)
        else:
            return None
    except subprocess.CalledProcessError:
        return None


def is_snowflake_cli_installed(version):
    """Check if the specific version of Snowflake CLI is already installed."""
    try:
        installed_version = pkg_resources.get_distribution("snowflake-cli").version
        return installed_version == version
    except pkg_resources.DistributionNotFound:
        return False


def install_snow_cli(python_version):
    """Install the appropriate Snowflake CLI version based on Python version if not already installed."""
    snow_cli_version = "3.1.0" if 3.10 <= float(python_version) <= 3.12 else "2.8.2"

    if is_snowflake_cli_installed(snow_cli_version):
        return

    package_name = f"snowflake-cli=={snow_cli_version}"
    try:
        subprocess.run(["pip", "install", package_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to install {package_name}: {e}")


def main(python_version):
    """Main function to handle installation and version reporting."""
    install_snow_cli(python_version)

    installed_version = get_installed_snowflake_cli_version()
    if installed_version:
        print(f"Installed Snowflake CLI version: {installed_version}")
    else:
        print(
            "No Snowflake CLI version is currently installed or could not retrieve version."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Install Snowflake CLI for a specific Python version."
    )
    parser.add_argument(
        "--pythonVersion",
        type=str,
        required=True,
        help="Specify the Python version (e.g., 3.10 or 3.9) to determine Snowflake CLI version.",
    )

    args = parser.parse_args()
    python_version = args.pythonVersion

    if python_version:
        main(python_version)
    else:
        print(
            "No version specified. Please provide a valid Python version using --pythonVersion."
        )
