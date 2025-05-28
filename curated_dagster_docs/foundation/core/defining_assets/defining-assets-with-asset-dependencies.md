
On this page

Asset definitions can depend on other asset definitions. The dependent asset is called the **downstream asset**, and the asset it depends on is the **upstream asset**.

## Defining basic dependencies [​](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies\#defining-basic-dependencies "Direct link to Defining basic dependencies")

You can define a dependency between two assets by passing the upstream asset to the `deps` parameter in the downstream asset's `@asset` decorator.

In this example, the asset `sugary_cereals` creates a new table ( `sugary_cereals`) by selecting records from the `cereals` table. Then the asset `shopping_list` creates a new table ( `shopping_list`) by selecting records from `sugary_cereals`:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def sugary_cereals() -> None:
    execute_query(
        "CREATE TABLE sugary_cereals AS SELECT * FROM cereals WHERE sugar_grams > 10"
    )

@dg.asset(deps=[sugary_cereals])
def shopping_list() -> None:
    execute_query("CREATE TABLE shopping_list AS SELECT * FROM sugary_cereals")

```

## Defining asset dependencies across code locations [​](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies\#defining-asset-dependencies-across-code-locations "Direct link to Defining asset dependencies across code locations")

Assets can depend on assets in different [code locations](https://docs.dagster.io/guides/deploy/code-locations/). In the following example, the `code_location_1_asset` asset produces a JSON string from a file in `code_location_1`:

```codeBlockLines_e6Vv
import json

import dagster as dg

@dg.asset
def code_location_1_asset():
    with open("/tmp/data/code_location_1_asset.json", "w+") as f:
        json.dump(5, f)

defs = dg.Definitions(assets=[code_location_1_asset])

```

In `code_location_2`, we can reference `code_location_1_asset` it via its asset key:

```codeBlockLines_e6Vv
import json

import dagster as dg

@dg.asset(deps=["code_location_1_asset"])
def code_location_2_asset():
    with open("/tmp/data/code_location_1_asset.json") as f:
        x = json.load(f)

    with open("/tmp/data/code_location_2_asset.json", "w+") as f:
        json.dump(x + 6, f)

```

- [Defining basic dependencies](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies#defining-basic-dependencies)
- [Defining asset dependencies across code locations](https://docs.dagster.io/guides/build/assets/defining-assets-with-asset-dependencies#defining-asset-dependencies-across-code-locations)
