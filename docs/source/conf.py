# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys


COLLECTORS_SRC_DIR = os.path.join("..", "..", "snowpark-checkpoints-collectors", "src")
CONFIGURATION_SRC_DIR = os.path.join("..", "..", "snowpark-checkpoints-configuration", "src")
HYPOTHESIS_SRC_DIR = os.path.join("..", "..", "snowpark-checkpoints-hypothesis", "src")
VALIDATORS_SRC_DIR = os.path.join("..", "..", "snowpark-checkpoints-validators", "src")

# Add each project to sys.path so Sphinx can find and import the modules during the build process.
sys.path.insert(0, os.path.abspath(COLLECTORS_SRC_DIR))
sys.path.insert(0, os.path.abspath(CONFIGURATION_SRC_DIR))
sys.path.insert(0, os.path.abspath(HYPOTHESIS_SRC_DIR))
sys.path.insert(0, os.path.abspath(VALIDATORS_SRC_DIR))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Snowpark Checkpoints Framework API Reference"
copyright = "2024, Snowflake Inc."
author = "Snowflake Inc."

VERSION_FILE = os.path.join("..", "..", "__version__.py")
__version__ = "0.0.0"  # The name of this variable must match with the one in __version__.py
with open(VERSION_FILE, encoding="utf-8") as f:
    exec(f.read())

version = __version__
release = __version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.coverage",
]
templates_path = ["_templates"]
autosummary_generate = True
autosummary_ignore_module_all = False
autosummary_imported_members = True
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "empty"
html_theme_path = [
    "_themes",
]
html_show_sourcelink = False
html_show_sphinx = False

# -- Options for autodoc -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

autoclass_content = "class"
autodoc_default_options = {
    "member-order": "alphabetical",
    "undoc-members": True,
    "show-inheritance": True,
}
