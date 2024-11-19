# Use this when GitHub actions want to display some information about the execution environment.

import os
import sys

from argparse import ArgumentParser

NLTAB = "\n     "
PATH_PARTS = os.getenv("PATH").split(os.pathsep)


parser = ArgumentParser(
    prog="showenv.py",
    description="Show useful execution environment values",
)
args = parser.parse_args()


print(f"PATH={NLTAB.join(PATH_PARTS)}")
print(f"Python version: {sys.version}")
print(f"Python executable: {sys.executable}")
