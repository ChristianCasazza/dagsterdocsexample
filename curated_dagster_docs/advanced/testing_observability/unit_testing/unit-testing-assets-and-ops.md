
On this page

Unit testing is essential for ensuring that computations function as intended. In the context of data pipelines, this can be particularly challenging. However, Dagster streamlines the process by enabling direct invocation of computations with specified input values and mocked resources, making it easier to verify that data transformations behave as expected.

While unit tests can't fully replace integration tests or manual review, they can catch a variety of errors with a significantly faster feedback loop.

This article covers how to write unit tests for assets with a variety of different input requirements.

note

Before you begin implementing unit tests, note that:

- Testing individual assets is generally recommended over unit testing entire jobs.
- Unit testing isn't recommended in cases where most of the business logic is encoded in an external system, such as an asset which directly invokes an external Databricks job.
- If you want to test your assets at runtime, you can use [asset checks](https://docs.dagster.io/guides/test/asset-checks) to verify the quality of data produced by your pipelines, communicate what the data is expected to do, and more.

## Unit test examples [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#unit-test-examples "Direct link to Unit test examples")

### Assets and ops without arguments [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#no-arguments "Direct link to Assets and ops without arguments")

The simplest assets to test are those with no arguments. In these cases, you can directly invoke definitions.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def loaded_file() -> str:
    with open("path.txt") as file:
        return file.read()

def test_loaded_file() -> None:
    assert loaded_file() == "contents"

```

### Assets with upstream dependencies [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#upstream-dependencies "Direct link to Assets with upstream dependencies")

If an asset has an upstream dependency, you can directly pass a value for that dependency when invoking the definition.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def loaded_file() -> str:
    with open("path.txt") as file:
        return file.read()

@dg.asset
def processed_file(loaded_file: str) -> str:
    return loaded_file.strip()

def test_processed_file() -> None:
    assert processed_file(" contents  ") == "contents"

```

### Assets with config [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#config "Direct link to Assets with config")

If an asset uses config, you can construct an instance of the required config object and pass it in directly.

```codeBlockLines_e6Vv
import dagster as dg

class FilepathConfig(dg.Config):
    path: str

@dg.asset
def loaded_file(config: FilepathConfig) -> str:
    with open(config.path) as file:
        return file.read()

def test_loaded_file() -> None:
    assert loaded_file(FilepathConfig(path="path1.txt")) == "contents1"
    assert loaded_file(FilepathConfig(path="path2.txt")) == "contents2"

```

### Assets with resources [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#resources "Direct link to Assets with resources")

If an asset uses a resource, it can be useful to create a mock instance of the resource to avoid interacting with external services.

```codeBlockLines_e6Vv
from unittest import mock

from dagster_aws.s3 import S3FileHandle, S3FileManager

import dagster as dg

@dg.asset
def loaded_file(file_manager: S3FileManager) -> str:
    return file_manager.read_data(S3FileHandle("bucket", "path.txt"))

def test_file() -> None:
    mocked_resource = mock.Mock(spec=S3FileManager)
    mocked_resource.read_data.return_value = "contents"

    assert loaded_file(mocked_resource) == "contents"
    assert mocked_resource.read_data.called_once_with(
        S3FileHandle("bucket", "path.txt")
    )

```

### Assets with context [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#context "Direct link to Assets with context")

If an asset uses a `context` argument, you can use `build_asset_context()` to construct a context object.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(partitions_def=dg.DailyPartitionsDefinition("2024-01-01"))
def loaded_file(context: dg.AssetExecutionContext) -> str:
    with open(f"path_{context.partition_key}.txt") as file:
        return file.read()

def test_loaded_file() -> None:
    context = dg.build_asset_context(partition_key="2024-08-16")
    assert loaded_file(context) == "Contents for August 16th, 2024"

```

### Assets with multiple parameters [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#multiple-parameters "Direct link to Assets with multiple parameters")

If an asset has multiple parameters, we recommended using keyword arguments for clarity.

```codeBlockLines_e6Vv
import dagster as dg

class SeparatorConfig(dg.Config):
    separator: str

@dg.asset
def processed_file(
    primary_file: str, secondary_file: str, config: SeparatorConfig
) -> str:
    return f"{primary_file}{config.separator}{secondary_file}"

def test_processed_file() -> None:
    assert (
        processed_file(
            primary_file="abc",
            secondary_file="def",
            config=SeparatorConfig(separator=","),
        )
        == "abc,def"
    )

```

## Running the tests [​](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops\#running-the-tests "Direct link to Running the tests")

Use `pytest` or your test runner of choice to run your unit tests. Navigate to the top-level project directory (the one that contains the tests directory) and run:

```codeBlockLines_e6Vv
pytest my_project_tests

```

- [Unit test examples](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#unit-test-examples)
  - [Assets and ops without arguments](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#no-arguments)
  - [Assets with upstream dependencies](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#upstream-dependencies)
  - [Assets with config](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#config)
  - [Assets with resources](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#resources)
  - [Assets with context](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#context)
  - [Assets with multiple parameters](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#multiple-parameters)
- [Running the tests](https://docs.dagster.io/guides/test/unit-testing-assets-and-ops#running-the-tests)
