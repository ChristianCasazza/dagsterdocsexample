
On this page

This reference shows usage of Dagster Pipes with other entities in the Dagster system. For a step-by-step walkthrough, refer to the [Dagster Pipes tutorial](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes).

## Specifying environment variables and extras [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#specifying-environment-variables-and-extras "Direct link to Specifying environment variables and extras")

When launching the subprocess, you may want to make environment variables or additional parameters available in the external process. Extras are arbitrary, user-defined parameters made available on the context object in the external process.

- External code in external\_code.py
- Dagster code in dagster\_code.py

In the external code, you can access extras via the `PipesContext` object:

```codeBlockLines_e6Vv
import os

import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    total_orders = len(orders_df)
    # get the Dagster Pipes context
    context = PipesContext.get()
    # get all extras provided by Dagster asset
    print(context.extras)
    # get the value of an extra
    print(context.get_extra("foo"))
    # get env var
    print(os.environ["MY_ENV_VAR_IN_SUBPROCESS"])

```

The `run` method to the `PipesSubprocessClient` resource also accepts `env` and `extras` , which allow you to specify environment variables and extra arguments when executing the subprocess:

Note: We're using `os.environ` in this example, but Dagster's recommendation is to use [`EnvVar`](https://docs.dagster.io/api/dagster/resources#dagster.EnvVar) in production.

```codeBlockLines_e6Vv
import shutil

import dagster as dg

@dg.asset
def subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
) -> dg.MaterializeResult:
    cmd = [shutil.which("python"), dg.file_relative_path(__file__, "external_code.py")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        extras={"foo": "bar"},
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "my_value",
        },
    ).get_materialize_result()

defs = dg.Definitions(
    assets=[subprocess_asset],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)

```

## Working with @asset\_check [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#working-with-asset_check "Direct link to Working with @asset_check")

Sometimes, you may not want to materialize an asset, but instead want to report a data quality check result. When your asset has data quality checks defined in [`@dg.asset_check`](https://docs.dagster.io/api/dagster/asset-checks#dagster.asset_check):

- External code in external\_code.py
- Dagster code in dagster\_code.py

From the external code, you can report to Dagster that an asset check has been performed via [`PipesContext`](https://docs.dagster.io/api/libraries/dagster-pipes#dagster_pipes.PipesContext). Note that `asset_key` in this case is required, and must match the asset key defined in [`@dg.asset_check`](https://docs.dagster.io/api/dagster/asset-checks#dagster.asset_check):

```codeBlockLines_e6Vv
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    # get the Dagster Pipes context
    context = PipesContext.get()
    # send structured metadata back to Dagster
    context.report_asset_check(
        asset_key="my_asset",
        passed=orders_df[["item_id"]].notnull().all().bool(),
        check_name="no_empty_order_check",
    )

```

On Dagster's side, the `PipesClientCompletedInvocation` object returned from `PipesSubprocessClient` includes a `get_asset_check_result` method, which you can use to access the [`AssetCheckResult`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult) event reported by the subprocess.

```codeBlockLines_e6Vv
import shutil

import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset_check(asset="my_asset")
def no_empty_order_check(
    context: dg.AssetCheckExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.AssetCheckResult:
    cmd = [\
        shutil.which("python"),\
        dg.file_relative_path(__file__, "external_code.py"),\
    ]

    results = pipes_subprocess_client.run(
        command=cmd, context=context.op_execution_context
    ).get_results()

    if not results:
        return dg.AssetCheckResult(passed=True)

    return dg.AssetCheckResult(passed=False)

defs = dg.Definitions(
    assets=[my_asset],
    asset_checks=[no_empty_order_check],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)

```

## Working with multi-assets [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#working-with-multi-assets "Direct link to Working with multi-assets")

Sometimes, you may invoke a single call to an API that results in multiple tables being updated, or you may have a single script that computes multiple assets. In these cases, you can use Dagster Pipes to report back on multiple assets at once.

- External code in external\_code.py
- Dagster code in dagster\_code.py

**Note**: When working with multi-assets, [`PipesContext`](https://docs.dagster.io/api/libraries/dagster-pipes#dagster_pipes.PipesContext) may only be called once per unique asset key. If called more than once, an error similar to the following will surface:

```codeBlockLines_e6Vv
Calling {method} with asset key {asset_key} is undefined. Asset has already been materialized, so no additional data can be reported for it

```

Instead, you’ll need to set the `asset_key` parameter for each instance of [`PipesContext`](https://docs.dagster.io/api/libraries/dagster-pipes#dagster_pipes.PipesContext):

```codeBlockLines_e6Vv
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    orders_df = pd.DataFrame(
        {"order_id": [1, 2, 3], "item_id": [432, 878, 102], "user_id": ["a", "b", "a"]}
    )
    total_orders = len(orders_df)
    total_users = orders_df["user_id"].nunique()

    # get the Dagster Pipes context
    context = PipesContext.get()
    # send structured metadata back to Dagster. asset_key is required when there are multiple assets
    context.report_asset_materialization(
        asset_key="orders", metadata={"total_orders": total_orders}
    )
    context.report_asset_materialization(
        asset_key="users", metadata={"total_users": total_users}
    )

```

In the Dagster code, you can use [`@dg.multi_asset`](https://docs.dagster.io/api/dagster/assets#dagster.multi_asset) to define a single asset that represents multiple assets. The `PipesClientCompletedInvocation` object returned from `PipesSubprocessClient` includes a `get_results` method, which you can use to access all the events, such as multiple [`AssetMaterializations`](https://docs.dagster.io/api/dagster/ops#dagster.AssetMaterialization) and [`AssetCheckResults`](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckResult), reported by the subprocess:

```codeBlockLines_e6Vv
import shutil

import dagster as dg

@dg.multi_asset(specs=[dg.AssetSpec("orders"), dg.AssetSpec("users")])
def subprocess_asset(
    context: dg.AssetExecutionContext, pipes_subprocess_client: dg.PipesSubprocessClient
):
    cmd = [\
        shutil.which("python"),\
        dg.file_relative_path(__file__, "external_code.py"),\
    ]
    return pipes_subprocess_client.run(command=cmd, context=context).get_results()

defs = dg.Definitions(
    assets=[subprocess_asset],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)

```

## Passing custom data [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#passing-custom-data "Direct link to Passing custom data")

Sometimes, you may want to pass data back from the external process for use in the orchestration code for purposes other than reporting directly to Dagster such as use in creating an output. In this example we use custom messages to create an I/O managed output that is returned from the asset.

- External code in external\_code.py
- Dagster code in dagster\_code.py

In the external code, we send messages using `report_custom_message`. The message can be any data that is JSON serializable.

```codeBlockLines_e6Vv
import pandas as pd
from dagster_pipes import PipesContext, open_dagster_pipes

def main():
    # get the Dagster Pipes context
    context = PipesContext.get()

    # compute the full orders data
    orders = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "item_id": [321, 654, 987],
            "order_details": [..., ..., ...],  # imagine large data,
            # and more columns
        }
    )

    # send a smaller table to be I/O managed by Dagster and passed to downstream assets
    summary_table = pd.DataFrame(orders[["order_id", "item_id"]])
    context.report_custom_message(summary_table.to_dict())

    context.report_asset_materialization(metadata={"total_orders": len(orders)})

```

In the Dagster code we receive custom messages using `get_custom_messages`.

```codeBlockLines_e6Vv
import shutil

import pandas as pd

import dagster as dg

@dg.asset
def subprocess_asset(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.Output[pd.DataFrame]:
    cmd = [shutil.which("python"), dg.file_relative_path(__file__, "external_code.py")]
    result = pipes_subprocess_client.run(
        command=cmd,
        context=context,
    )

    # a small summary table gets reported as a custom message
    messages = result.get_custom_messages()
    if len(messages) != 1:
        raise Exception("summary not reported")

    summary_df = pd.DataFrame(messages[0])

    # grab any reported metadata off of the materialize result
    metadata = result.get_materialize_result().metadata

    # return the summary table to be loaded by Dagster for downstream assets
    return dg.Output(
        value=summary_df,
        metadata=metadata,
    )

defs = dg.Definitions(
    assets=[subprocess_asset],
    resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
)

```

## Passing rich metadata to Dagster [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#passing-rich-metadata-to-dagster "Direct link to Passing rich metadata to Dagster")

Dagster supports rich metadata types such as [`TableMetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.TableMetadataValue), [`UrlMetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.UrlMetadataValue), and [`JsonMetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.JsonMetadataValue). However, because the `dagster-pipes` package doesn't have a direct dependency on `dagster`, you'll need to pass a raw dictionary back to Dagster with a specific format. For a given metadata type, you will need to specify a dictionary with the following keys:

- `type`: The type of metadata value, such as `table`, `url`, or `json`.
- `raw_value`: The actual value of the metadata.

Below are examples of specifying data for all supported metadata types. Float, integer, boolean, string, and null metadata objects can be passed directly without the need for a dictionary.

### Examples for complex metadata types [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#examples-for-complex-metadata-types "Direct link to Examples for complex metadata types")

#### URL Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#url-metadata "Direct link to URL Metadata")

```codeBlockLines_e6Vv
def get_url(args): ...

context = ...

# start_url
# Within the Dagster pipes subprocess:
url = "http://example.com"
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"url_meta": {"type": "url", "raw_value": url}},
)
# end_url

```

#### Path Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#path-metadata "Direct link to Path Metadata")

```codeBlockLines_e6Vv
def get_path(args): ...

context = ...

# start_path
# Within the Dagster pipes subprocess:
path = "/path/to/file.txt"
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"path_meta": {"type": "path", "raw_value": path}},
)
# end_path

```

#### Notebook Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#notebook-metadata "Direct link to Notebook Metadata")

```codeBlockLines_e6Vv
def get_notebook_path(args): ...

context = ...

# start_notebook
# Within the Dagster pipes subprocess:
notebook_path = "/path/to/notebook.ipynb"
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"notebook_meta": {"type": "notebook", "raw_value": notebook_path}},
)
# end_notebook

```

#### JSON Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#json-metadata "Direct link to JSON Metadata")

```codeBlockLines_e6Vv
def get_json_data(args): ...

context = ...

# start_json
# Within the Dagster pipes subprocess:
json_data = ["item1", "item2", "item3"]
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"json_meta": {"type": "json", "raw_value": json_data}},
)
# end_json

```

#### Markdown Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#markdown-metadata "Direct link to Markdown Metadata")

```codeBlockLines_e6Vv
def get_markdown_content(args): ...

context = ...

# start_markdown
# Within the Dagster pipes subprocess:
markdown_content = "# Header\nSome **bold** text"
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"md_meta": {"type": "md", "raw_value": markdown_content}},
)
# end_markdown

```

#### Table Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#table-metadata "Direct link to Table Metadata")

```codeBlockLines_e6Vv
context = ...

# start_table
# Within the Dagster pipes subprocess:
schema = [\
    {\
        "name": "column1",\
        "type": "string",\
        "description": "The first column",\
        "tags": {"source": "source1"},\
        "constraints": {"unique": True},\
    },\
    {\
        "name": "column2",\
        "type": "int",\
        "description": "The second column",\
        "tags": {"source": "source2"},\
        "constraints": {"min": 0, "max": 100},\
    },\
]
records = [\
    {"column1": "foo", "column2": 1},\
    {"column1": "bar", "column2": 2},\
]
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={
        "table_meta": {
            "type": "table",
            "raw_value": {"schema": schema, "records": records},
        }
    },
)
# end_table

```

#### Table Schema Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#table-schema-metadata "Direct link to Table Schema Metadata")

```codeBlockLines_e6Vv
context = ...

# start_table_schema
# Within the Dagster pipes subprocess:
schema = [\
    {\
        "name": "column1",\
        "type": "string",\
        "description": "The first column",\
        "tags": {"source": "source1"},\
        "constraints": {"unique": True},\
    },\
    {\
        "name": "column2",\
        "type": "int",\
        "description": "The second column",\
        "tags": {"source": "source2"},\
        "constraints": {"min": 0, "max": 100},\
    },\
]

# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={
        "table_meta": {
            "type": "table_schema",
            "raw_value": schema,
        }
    },
)
# end_table_schema

```

#### Table Column Lineage Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#table-column-lineage-metadata "Direct link to Table Column Lineage Metadata")

```codeBlockLines_e6Vv
# Within the Dagster pipes subprocess:
lineage = {
    "a": [{"asset_key": "upstream", "column": "column1"}],
    "b": [{"asset_key": "upstream", "column": "column2"}],
}
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={
        "lineage_meta": {
            "type": "table_column_lineage",
            "raw_value": {"table_column_lineage": lineage},
        }
    },
)

```

#### Timestamp Metadata [​](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference\#timestamp-metadata "Direct link to Timestamp Metadata")

```codeBlockLines_e6Vv
# Within the Dagster pipes subprocess:
timestamp = 1234567890
# Then, when reporting the asset materialization:
context.report_asset_materialization(
    asset_key="foo",
    metadata={"timestamp_meta": {"type": "timestamp", "raw_value": timestamp}},
)

```

- [Specifying environment variables and extras](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#specifying-environment-variables-and-extras)
- [Working with @asset\_check](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#working-with-asset_check)
- [Working with multi-assets](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#working-with-multi-assets)
- [Passing custom data](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#passing-custom-data)
- [Passing rich metadata to Dagster](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#passing-rich-metadata-to-dagster)
  - [Examples for complex metadata types](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#examples-for-complex-metadata-types)
    - [URL Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#url-metadata)
    - [Path Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#path-metadata)
    - [Notebook Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#notebook-metadata)
    - [JSON Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#json-metadata)
    - [Markdown Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#markdown-metadata)
    - [Table Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#table-metadata)
    - [Table Schema Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#table-schema-metadata)
    - [Table Column Lineage Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#table-column-lineage-metadata)
    - [Timestamp Metadata](https://docs.dagster.io/guides/build/external-pipelines/using-dagster-pipes/reference#timestamp-metadata)
