# Contributing to Snowflake Snowpark Checkpoints

Hi, thank you for taking the time to improve Snowflake's Snowpark Checkpoints.

## I have a feature request, or a bug report to submit

Many questions can be answered by checking our [docs][docs] or looking for already existing bug reports and enhancement requests on our [issue tracker][issue tracker].

Please start by checking these first!

## Nobody else had my idea/issue

In that case we'd love to hear from you!
Please [open a new issue][open issue] to get in touch with us.


## I'd like to contribute the bug fix or feature myself

We encourage everyone to first [open a new issue][open issue] to discuss any feature work or bug fixes with one of the maintainers.
The following should help guide contributors through potential pitfalls.


## Contributor License Agreement ("CLA")

We require our contributors to sign a CLA, available at https://github.com/snowflakedb/CLA/blob/main/README.md. A Github Actions bot will assist you when you open a pull request.


### Setup a development environment

#### Fork the repository and then clone the forked repo

```bash
git clone <YOUR_FORKED_REPO>
cd snowpark-checkpoints
```

#### Install the library in edit mode and install its dependencies

- Create a new Python virtual environment with any Python version that we support.
  - The Snowpark Checkpoints API supports **Python 3.9, Python 3.10, and Python 3.11**.

    In this case you can try using Conda. 

    ```bash
    conda create --name snowpark-dev -c https://repo.anaconda.com/pkgs/snowflake python=3.11 -y
    ```

- Activate the new Python virtual environment. For example,

  ```bash
  conda activate snowpark-dev
  ```

- Go to the cloned repository root folder.
  - To install the Snowpark Checkpoints API in edit/development mode, use:

      ```bash
      python -m pip install -e ".[development]"
      ```

  The `-e` tells `pip` to install the library in [edit, or development mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs).

#### Setup your IDE

You can use PyCharm, VS Code, or any other IDE.
The following steps assume you use PyCharm, VS Code or any other similar IDE.

##### Download and install PyCharm

Download the newest community version of [PyCharm](https://www.jetbrains.com/pycharm/download/)
and follow the [installation instructions](https://www.jetbrains.com/help/pycharm/installation-guide.html).

##### Download and install VS Code

Download and install the latest version of [VS Code](https://code.visualstudio.com/download)

##### Setup project

Open project and browse to the cloned git directory. Then right-click the directory `src` in PyCharm
and "Mark Directory as" -> "Source Root". **NOTE**: VS Code doesn't have "Source Root" so you can skip this step if you use VS Code.

##### Setup Python Interpreter

[Configure PyCharm interpreter][config pycharm interpreter] or [Configure VS Code interpreter][config vscode interpreter] to use the previously created Python virtual environment.

## Tests

The [README under tests folder](snowpark-checkpoints-testing/README.md) tells you how to set up to run tests.


## Why do my PR tests fail?

If this happens to you do not panic! Any PRs originating from a fork will fail some automated tests. This is because
forks do not have access to our repository's secrets. A maintainer will manually review your changes then kick off
the rest of our testing suite. Feel free to tag [@snowflakedb/snowpark-cr-team](https://github.com/orgs/snowflakedb/teams/snowpark-cr-team)
if you feel like we are taking too long to get to your PR.

## Snowpark Checkpoints Folder structure
Following tree diagram shows the high-level structure of the Snowpark Checkpoints.

```

snowpark-checkpoints
├── Demos
│   ├── JupyterNotebookCheckpoints
│   ├── pyspark
│   └── snowpark
├── snowpark-checkpoints-collectors
│   ├── src
│   └── test
├── snowpark-checkpoints-configuration
│   ├── src
│   └── test
├── snowpark-checkpoints-hypothesis
│   ├── src
│   └── test
├── snowpark-checkpoints-testing
│   ├── src
│   └── test
└── snowpark-checkpoints-validators
    ├── src
    └── test
```

------

[docs]: https://docs.snowflake.com/en/developer-guide/snowpark-checkpoints-library
[issue tracker]: https://github.com/snowflakedb/snowpark-checkpoints/issues
[open issue]: https://github.com/snowflakedb/snowpark-checkpoints/issues/new/choose
[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
[config vscode interpreter]: https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter
