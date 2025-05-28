
On this page

The easiest way to start building a Dagster project is by using the `dagster project` CLI. This CLI tool helps generate files and folder structures that enable you to quickly get started with Dagster.

## Step 1: Bootstrap a new project [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#step-1-bootstrap-a-new-project "Direct link to Step 1: Bootstrap a new project")

note

If you don't already have Dagster installed, verify you meet the [installation requirements](https://docs.dagster.io/getting-started/install) before
continuing.

You can scaffold a new project using the default project skeleton, or start with one of the official Dagster examples.

To learn more about the default files in a Dagster project, refer to the [Dagster project file reference](https://docs.dagster.io/guides/build/projects/dagster-project-file-reference).

- Default project skeleton
- Official example

### Using the default project skeleton [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#using-the-default-project-skeleton "Direct link to Using the default project skeleton")

To get started, run:

```codeBlockLines_e6Vv
pip install dagster
dagster project scaffold --name my-dagster-project

```

The `dagster project scaffold` command generates a folder structure with a single Dagster code location and other files, such as `pyproject.toml` and `setup.py`. This takes care of setting things up with an empty project, enabling you to quickly get started.

### Using an official example [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#using-an-official-example "Direct link to Using an official example")

To get started using an official Dagster example, run:

```codeBlockLines_e6Vv
pip install dagster
dagster project from-example \
  --name my-dagster-project \
  --example quickstart_etl

```

The command `dagster project from-example` downloads one of the official Dagster examples to the current directory. This command enables you to quickly bootstrap your project with an officially maintained example.

For more info about the examples, visit the [Dagster GitHub repository](https://github.com/dagster-io/dagster/tree/master/examples) or use `dagster project list-examples`.

## Step 2: Install project dependencies [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#step-2-install-project-dependencies "Direct link to Step 2: Install project dependencies")

The newly generated `my-dagster-project` directory is a fully functioning [Python package](https://docs.python.org/3/tutorial/modules.html#packages) and can be installed with `pip`.

To install it as a package and its Python dependencies, run:

```codeBlockLines_e6Vv
pip install -e ".[dev]"

```

:::

Using the `--editable` ( `-e`) flag instructs `pip` to install your code location as a Python package in

["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs)

so that as you develop, local code changes are automatically applied.

:::

## Step 3: Start the Dagster UI [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#step-3-start-the-dagster-ui "Direct link to Step 3: Start the Dagster UI")

To start the [Dagster UI](https://docs.dagster.io/guides/operate/webserver), run:

```codeBlockLines_e6Vv
dagster dev

```

**Note**: This command also starts the [Dagster daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon). Refer to the [Running Dagster locally guide](https://docs.dagster.io/guides/deploy/deployment-options/running-dagster-locally) for more info.

Use your browser to open [http://localhost:3000](http://localhost:3000/) to view the project.

## Step 4: Development [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#step-4-development "Direct link to Step 4: Development")

- [Adding new Python dependencies](https://docs.dagster.io/guides/build/projects/creating-a-new-project#adding-new-python-dependencies)
- [Environment variables and secrets](https://docs.dagster.io/guides/build/projects/creating-a-new-project#using-environment-variables-and-secrets)
- [Unit testing](https://docs.dagster.io/guides/build/projects/creating-a-new-project#adding-and-running-unit-tests)

### Adding new Python dependencies [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#adding-new-python-dependencies "Direct link to Adding new Python dependencies")

You can specify new Python dependencies in `setup.py`.

### Using environment variables and secrets [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#using-environment-variables-and-secrets "Direct link to Using environment variables and secrets")

Environment variables, which are key-value pairs configured outside your source code, allow you to dynamically modify application behavior depending on environment.

Using environment variables, you can define various configuration options for your Dagster application and securely set up secrets. For example, instead of hard-coding database credentials - which is bad practice and cumbersome for development - you can use environment variables to supply user details. This allows you to parameterize your pipeline without modifying code or insecurely storing sensitive data.

For more information and examples, see " [Using environment variables and secrets](https://docs.dagster.io/guides/deploy/using-environment-variables-and-secrets)".

### Adding and running unit tests [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#adding-and-running-unit-tests "Direct link to Adding and running unit tests")

Tests can be added in the `my_dagster_project_tests` directory and run using `pytest`:

```codeBlockLines_e6Vv
pytest my_dagster_project_tests

```

## Next steps [​](https://docs.dagster.io/guides/build/projects/creating-a-new-project\#next-steps "Direct link to Next steps")

Once your project is ready to move to production, check out our recommendations for [transitioning data pipelines from development to production](https://docs.dagster.io/guides/deploy/dev-to-prod).

Check out the following resources to learn more about deployment options:

- [Dagster+](https://docs.dagster.io/dagster-plus/) \- Deploy using Dagster-managed infrastructure
- [Your own infrastructure](https://docs.dagster.io/guides/deploy/) \- Deploy to your infrastructure, such as Docker, Kubernetes, Amazon Web Services, etc.

- [Step 1: Bootstrap a new project](https://docs.dagster.io/guides/build/projects/creating-a-new-project#step-1-bootstrap-a-new-project)
  - [Using the default project skeleton](https://docs.dagster.io/guides/build/projects/creating-a-new-project#using-the-default-project-skeleton)
  - [Using an official example](https://docs.dagster.io/guides/build/projects/creating-a-new-project#using-an-official-example)
- [Step 2: Install project dependencies](https://docs.dagster.io/guides/build/projects/creating-a-new-project#step-2-install-project-dependencies)
- [Step 3: Start the Dagster UI](https://docs.dagster.io/guides/build/projects/creating-a-new-project#step-3-start-the-dagster-ui)
- [Step 4: Development](https://docs.dagster.io/guides/build/projects/creating-a-new-project#step-4-development)
  - [Adding new Python dependencies](https://docs.dagster.io/guides/build/projects/creating-a-new-project#adding-new-python-dependencies)
  - [Using environment variables and secrets](https://docs.dagster.io/guides/build/projects/creating-a-new-project#using-environment-variables-and-secrets)
  - [Adding and running unit tests](https://docs.dagster.io/guides/build/projects/creating-a-new-project#adding-and-running-unit-tests)
- [Next steps](https://docs.dagster.io/guides/build/projects/creating-a-new-project#next-steps)
