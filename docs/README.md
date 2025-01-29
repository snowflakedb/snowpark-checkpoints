# Snowpark Checkpoints Documentation

## How to build the API Reference documentation

1. Create a Python virtual environment if you don't have one already and activate it:

    ```shell
    python3 -m venv .venv
    source .venv/bin/activate
    ```

2. Install the required dependencies:

    ```shell
    pip install hatch
    ```

3. Build the documentation from the root of the repository using `hatch`:

    If you are not in the root of the repository, navigate to it first:

    ```bash
    cd $(git rev-parse --show-toplevel)
    ```  

    Then, run the following command to build the documentation:

    ```bash
    hatch run docs:build
    ```
    
    Alternatively, to build the documentation with a custom theme for testing purposes, you can run the following command:
    
    ```bash
    hatch run docs:build_pretty
    ```

4. Open the file `docs/build/html/index.html` in a web browser.

## Important files and directories

* `docs/source/index.rst`: Specify which `.rst` to include in the `index.html` landing page.
* `docs/source/conf.py`: The configuration parameters for Sphinx and autosummary.
* `docs/source/_templates/`: Directory containing JINJA templates used by autosummary.
