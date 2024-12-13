# Snowpark Checkpoints Documentation

## How to build the API Reference documentation

1. Build the documentation using `hatch`:

```bash
hatch run docs:build
```

Alternatively, to build the documentation with a custom theme for testing purposes, you can run the following command:

```bash
hatch run docs:build_pretty
```

2. Open the file `docs/build/html/index.html` in a web browser.

## Important files and directories

* `docs/source/index.rst`: Specify which `.rst` to include in the `index.html` landing page.
* `docs/source/conf.py`: The configuration parameters for Sphinx and autosummary.
* `docs/source/_templates/`: Directory containing JINJA templates used by autosummary.
