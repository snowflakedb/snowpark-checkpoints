import platform
import subprocess
import socket
import requests


def get_public_ip():
    services = [
        "https://api.ipify.org",
        "https://ifconfig.me",
        "https://ip.seeip.org",
        "https://ipecho.net/plain",
    ]

    for service in services:
        try:
            ip = requests.get(service, timeout=5).text.strip()
            return ip
        except:  # noqa: E722
            continue

    return "Could not retrieve public IP"


def get_system_info():
    os_type = platform.system().lower()

    print(f"OS: {os_type}")
    print(f"Hostname: {socket.gethostname()}")
    print(f"Current User: {platform.node()}")

    public_ip = get_public_ip()
    print(f"Public IP: {public_ip}")

    try:
        if os_type in ["linux", "darwin"]:
            subprocess.run(["date"], check=True)
            subprocess.run(
                ["hostname", "-I"]
                if os_type == "linux"
                else ["ipconfig", "getifaddr", "en0"],
                check=True,
            )
        elif os_type == "windows":
            subprocess.run("date /t", shell=True, check=True)
            subprocess.run("ipconfig", shell=True, check=True)
    except Exception as e:
        print(f"Error retrieving system information: {e}")


if __name__ == "__main__":
    get_system_info()
