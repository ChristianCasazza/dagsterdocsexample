
On this page

[Assets](https://docs.dagster.io/guides/build/assets/) feature prominently in the Dagster UI. Attaching information to assets allows you to understand where they're stored, what they contain, and how they should be organized.

Using metadata in Dagster, you can:

- Attach ownership information
- Organize assets with tags
- Attach rich, complex information such as a Markdown description, a table schema, or a time series
- Link assets with their source code

## Adding owners to assets [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#owners "Direct link to Adding owners to assets")

In a large organization, it's important to know which individuals and teams are responsible for a given data asset.

```codeBlockLines_e6Vv
import dagster as dg

# You can attach owners via the `owners` argument on the `@asset` decorator.
@dg.asset(owners=["richard.hendricks@hooli.com", "team:data-eng"])
def my_asset(): ...

# You can also use `owners` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset", owners=["bighead@hooli.com", "team:roof", "team:corpdev"]
)

```

note

`owners` must either be an email address or a team name prefixed by `team:` (e.g. `team:data-eng`).

tip

With Dagster+ Pro, you can create asset-based alerts that automatically notify an asset's owners when triggered. Refer to the [Dagster+ alert documentation](https://docs.dagster.io/dagster-plus/features/alerts) for more information.

## Organizing assets with tags [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#tags "Direct link to Organizing assets with tags")

[**Tags**](https://docs.dagster.io/guides/build/assets/metadata-and-tags/tags) are the primary way to organize assets in Dagster. You can attach several tags to an asset when it's defined, and they will appear in the UI. You can also use tags to search and filter for assets in the Asset catalog. They're structured as key-value pairs of strings.

Here's an example of some tags you might apply to an asset:

```codeBlockLines_e6Vv
{"domain": "marketing", "pii": "true"}

```

As with `owners`, you can pass a dictionary of tags to the `tags` argument when defining an asset:

```codeBlockLines_e6Vv
import dagster as dg

# You can attach tags via the `tags` argument on the `@asset` decorator.
@dg.asset(tags={"domain": "marketing", "pii": "true"})
def my_asset(): ...

# You can also use `tags` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset", tags={"domain": "legal", "sensitive": ""}
)

```

Keep in mind that tags must contain only strings as keys and values. Additionally, the Dagster UI will render tags with the empty string as a "label" rather than a key-value pair.

## Attaching metadata to assets [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#attaching-metadata "Direct link to Attaching metadata to assets")

**Metadata** allows you to attach rich information to the asset, like a Markdown description, a table schema, or a time series. Metadata is more flexible than tags, as it can store more complex information.

Metadata can be attached to an asset at definition time, when the code is first imported, or at runtime when an asset is materialized.

### At definition time [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#definition-time-metadata "Direct link to At definition time")

Using definition metadata to describe assets can make it easy to provide context for you and your team. This metadata could be descriptions of the assets, the types of assets, or links to relevant documentation.

```codeBlockLines_e6Vv
import dagster as dg

# You can attach metadata via the `metadata` argument on the `@asset` decorator.
@dg.asset(
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://..."),
        "snippet": dg.MetadataValue.md("# Embedded markdown\n..."),
    }
)
def my_asset(): ...

# You can also use `metadata` with `AssetSpec`
my_external_asset = dg.AssetSpec(
    "my_external_asset",
    metadata={
        "link_to_docs": dg.MetadataValue.url("https://..."),
        "snippet": dg.MetadataValue.md("# Embedded markdown\n..."),
    },
)

```

To learn more about the different types of metadata you can attach, see the [`MetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) API docs.

Some metadata keys will be given special treatment in the Dagster UI. See the [Standard metadata types](https://docs.dagster.io/guides/build/assets/metadata-and-tags#standard-metadata-types) section for more information.

### At runtime [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#runtime-metadata "Direct link to At runtime")

With runtime metadata, you can surface information about an asset's materialization, such as how many records were processed or when the materialization occurred. This allows you to update an asset's information when it changes and track historical metadata as a time series.

To attach materialization metadata to an asset, returning a [`MaterializeResult`](https://docs.dagster.io/api/dagster/assets#dagster.MaterializeResult) object containing a `metadata` parameter. This parameter accepts a dictionary of key/value pairs, where keys must be a string.

When specifying values, use the [`MetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.MetadataValue) utility class to wrap the data to ensure it displays correctly in the UI. Values can also be primitive Python types, which Dagster will convert to the appropriate `MetadataValue`.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def my_asset():
    # Asset logic goes here
    return dg.MaterializeResult(
        metadata={
            # file_size_kb will be rendered as a time series
            "file_size_kb": dg.MetadataValue.int(...)
        }
    )

```

note

Numerical metadata is treated as a time series in the Dagster UI.

## Standard metadata types [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#standard-metadata-types "Direct link to Standard metadata types")

The following metadata keys are given special treatment in the Dagster UI.

| Key | Description |
| --- | --- |
| `dagster/uri` | **Type:** `str`<br> The URI for the asset, for example: "s3://my\_bucket/my\_object" |
| `dagster/column_schema` | **Type:** [`TableSchema`](https://docs.dagster.io/api/dagster/metadata#dagster.TableSchema)<br> For an asset that's a table, the schema of the columns in the table. Refer to the [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column) section for details. |
| `dagster/column_lineage` | **Type:** [`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage)<br> For an asset that's a table, the lineage of column inputs to column outputs for the table. Refer to the [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column) section for details. |
| `dagster/row_count` | **Type:** `int`<br> For an asset that's a table, the number of rows in the table. Refer to the Table metadata documentation for details. |
| `dagster/partition_row_count` | **Type:** `int`<br> For a partition of an asset that's a table, the number of rows in the partition. |
| `dagster/table_name` | **Type:** `str`<br> A unique identifier for the table/view, typically fully qualified. For example, my\_database.my\_schema.my\_table |
| `dagster/code_references` | **Type:** [`CodeReferencesMetadataValue`](https://docs.dagster.io/api/dagster/metadata#dagster.CodeReferencesMetadataValue)<br> A list of [code references](https://docs.dagster.io/guides/build/assets/metadata-and-tags#source-code) for the asset, such as file locations or references to GitHub URLs. Should only be provided in definition-level metadata, not materialization metadata. |

## Table and column metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#table-column "Direct link to Table and column metadata")

Two of the most powerful metadata types are [`TableSchema`](https://docs.dagster.io/api/dagster/metadata#dagster.TableSchema) and [`TableColumnLineage`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumnLineage). These metadata types allow stakeholders to view the schema of a table right within Dagster, and, in Dagster+, navigate to the [Asset catalog](https://docs.dagster.io/dagster-plus/features/asset-catalog/) with the column lineage.

### Table schema metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#table-schema "Direct link to Table schema metadata")

The following example attaches [table and column schema metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/table-metadata) at both definition time and runtime:

```codeBlockLines_e6Vv
import dagster as dg

# Definition-time metadata
# Here, we know the schema of the asset, so we can attach it to the asset decorator
@dg.asset(
    deps=["source_bar", "source_baz"],
    metadata={
        "dagster/column_schema": dg.TableSchema(
            columns=[\
                dg.TableColumn(\
                    "name",\
                    "string",\
                    description="The name of the person",\
                ),\
                dg.TableColumn(\
                    "age",\
                    "int",\
                    description="The age of the person",\
                ),\
            ]
        )
    },
)
def my_asset(): ...

# Runtime metadata
# Here, the schema isn't known until the asset is materialized
@dg.asset(deps=["source_bar", "source_baz"])
def my_other_asset():
    column_names = ...
    column_types = ...

    columns = [\
        dg.TableColumn(name, column_type)\
        for name, column_type in zip(column_names, column_types)\
    ]

    return dg.MaterializeResult(
        metadata={"dagster/column_schema": dg.TableSchema(columns=columns)}
    )

```

There are several data types and constraints available on [`TableColumn`](https://docs.dagster.io/api/dagster/metadata#dagster.TableColumn) objects. For more information, see the API documentation.

### Column lineage metadata [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#column-lineage "Direct link to Column lineage metadata")

tip

Many integrations such as [dbt](https://docs.dagster.io/integrations/libraries/dbt) automatically attach column lineage metadata out-of-the-box.

[Column lineage metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags/column-level-lineage) is a powerful way to track how columns in a table are derived from other columns:

Table column lineage metadata

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(deps=["source_bar", "source_baz"])
def my_asset():
    # The following TableColumnLineage models 3 tables:
    # source_bar, source_baz, and my_asset
    # With the following column dependencies:
    # - my_asset.new_column_foo depends on:
    #   - source_bar.column_bar
    #   - source_baz.column_baz
    # - my_asset.new_column_qux depends on:
    #   - source_bar.column_quuz
    return dg.MaterializeResult(
        metadata={
            "dagster/column_lineage": dg.TableColumnLineage(
                deps_by_column={
                    "new_column_foo": [\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_bar"),\
                            column_name="column_bar",\
                        ),\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_baz"),\
                            column_name="column_baz",\
                        ),\
                    ],
                    "new_column_qux": [\
                        dg.TableColumnDep(\
                            asset_key=dg.AssetKey("source_bar"),\
                            column_name="column_quuz",\
                        ),\
                    ],
                }
            )
        }
    )

```

tip

Dagster+ provides rich visualization and navigation of column lineage in the Asset catalog. Refer to the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/features/asset-catalog/) for more information.

## Linking assets with source code [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#source-code "Direct link to Linking assets with source code")

info

This feature is considered in a beta stage. It is still being tested and may change. For more information, see the [API lifecycle stages documentation](https://docs.dagster.io/api/api-lifecycle/api-lifecycle-stages).

To link assets with their source code, you can attach a code reference. Code references are a type of metadata that allow you to easily view those assets' source code from the Dagster UI, both in local development and in production.

tip

Many integrations, such as [dbt](https://docs.dagster.io/integrations/libraries/dbt/reference#attaching-code-reference-metadata), support this capability.

### Attaching Python code references for local development [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#python-references "Direct link to Attaching Python code references for local development")

Dagster can automatically attach code references to assets during local development:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

defs = dg.Definitions(
    # with_source_code_references() automatically attaches the proper metadata
    assets=dg.with_source_code_references([my_asset, another_asset])
)

```

### Customizing code references [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#custom-references "Direct link to Customizing code references")

If you want to customize how code references are attached - such as when you are building [domain-specific languages with asset factories](https://docs.dagster.io/guides/build/assets/creating-asset-factories) \- you can manually add the `dagster/code_references` metadata to asset definitions:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    metadata={
        "dagster/code_references": dg.CodeReferencesMetadataValue(
            code_references=[\
                dg.LocalFileCodeReference(\
                    file_path="/path/to/source.yaml",\
                    # Label and line number are optional\
                    line_number=1,\
                    label="Model YAML",\
                )\
            ]
        )
    }
)
def my_asset_modeled_in_yaml(): ...

defs = dg.Definitions(assets=dg.with_source_code_references([my_asset_modeled_in_yaml]))

```

### Attaching code references in production [​](https://docs.dagster.io/guides/build/assets/metadata-and-tags\#production-references "Direct link to Attaching code references in production")

- Dagster+
- OSS

Dagster+ can automatically annotate assets with code references to source control, such as GitHub or GitLab.

```codeBlockLines_e6Vv
from dagster_cloud.metadata.source_code import link_code_references_to_git_if_cloud

import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

defs = dg.Definitions(
    assets=link_code_references_to_git_if_cloud(
        assets_defs=dg.with_source_code_references([my_asset, another_asset]),
        # This function also supports customizable path mapping, but usually
        # the defaults are fine.
    )
)

```

If you aren't using Dagster+, you can annotate your assets with code references to source control, but it requires manual mapping:

```codeBlockLines_e6Vv
import os
from pathlib import Path

import dagster as dg

@dg.asset
def my_asset(): ...

@dg.asset
def another_asset(): ...

assets = dg.with_source_code_references([my_asset, another_asset])

defs = dg.Definitions(
    assets=dg.link_code_references_to_git(
        assets_defs=assets,
        git_url="https://github.com/dagster-io/dagster",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__),
            file_anchor_path_in_repository="src/repo.py",
        ),
    )
    # Only link to GitHub in production. link locally otherwise
    if bool(os.getenv("IS_PRODUCTION"))
    else assets
)

```

`link_code_references_to_git` currently supports GitHub and GitLab repositories. It also supports customization of how file paths are mapped; see the `AnchorBasedFilePathMapping` API docs for more information.

- [Adding owners to assets](https://docs.dagster.io/guides/build/assets/metadata-and-tags#owners)
- [Organizing assets with tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags#tags)
- [Attaching metadata to assets](https://docs.dagster.io/guides/build/assets/metadata-and-tags#attaching-metadata)
  - [At definition time](https://docs.dagster.io/guides/build/assets/metadata-and-tags#definition-time-metadata)
  - [At runtime](https://docs.dagster.io/guides/build/assets/metadata-and-tags#runtime-metadata)
- [Standard metadata types](https://docs.dagster.io/guides/build/assets/metadata-and-tags#standard-metadata-types)
- [Table and column metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-column)
  - [Table schema metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#table-schema)
  - [Column lineage metadata](https://docs.dagster.io/guides/build/assets/metadata-and-tags#column-lineage)
- [Linking assets with source code](https://docs.dagster.io/guides/build/assets/metadata-and-tags#source-code)
  - [Attaching Python code references for local development](https://docs.dagster.io/guides/build/assets/metadata-and-tags#python-references)
  - [Customizing code references](https://docs.dagster.io/guides/build/assets/metadata-and-tags#custom-references)
  - [Attaching code references in production](https://docs.dagster.io/guides/build/assets/metadata-and-tags#production-references)
