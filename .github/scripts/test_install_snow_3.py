import subprocess
import re


def get_snow_cli_version():
    try:
        result = subprocess.run(
            "snow --version", capture_output=True, text=True, shell=True
        )
        version_match = re.search(
            r"Snowflake CLI version: (\d+\.\d+\.\d+)", result.stdout
        )
        if version_match:
            return version_match.group(1)
    except subprocess.CalledProcessError:
        return None


def install_toml():
    subprocess.run(["pip", "install", "toml"], check=True)


def validate_snow_cli_installed():
    min_version = "3.1.0"
    current_version = get_snow_cli_version()

    if current_version is None:
        print(f"Snowflake CLI version is not installed. Installing...")
        print(f"Installing Snowflake CLI version {min_version}...")
        subprocess.run(["pip", "install", f"snowflake-cli=={min_version}"], check=True)
        current_version = get_snow_cli_version()
        print(f"Snowflake CLI version: {current_version}")
        return
    elif current_version < min_version:
        print(
            f"‼️Snowflake CLI current version found {current_version} It should be at least {min_version}. Upgrading..."
        )
        subprocess.run(
            ["pip", "install", f"snowflake-cli=={min_version}", "--force-reinstall"],
            check=True,
        )
        current_version = get_snow_cli_version()
        print(f"Snowflake CLI version: {current_version}")
    else:
        print(f"Snowflake CLI version: {current_version} is valid")
        return


if __name__ == "__main__":
    validate_snow_cli_installed()
    install_toml()
