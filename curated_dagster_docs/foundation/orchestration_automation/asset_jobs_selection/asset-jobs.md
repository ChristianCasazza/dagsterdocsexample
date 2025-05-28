
On this page

Jobs are the main unit of execution and monitoring for [asset definitions](https://docs.dagster.io/guides/build/assets/defining-assets) in Dagster. An asset job is a type of job that targets a [selection of assets](https://docs.dagster.io/guides/build/assets/asset-selection-syntax) and can be launched:

- Manually from the Dagster UI
- At fixed intervals, by [schedules](https://docs.dagster.io/guides/automate/schedules)
- When external changes occur, using [sensors](https://docs.dagster.io/guides/automate/sensors)

## Creating asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#creating-asset-jobs "Direct link to Creating asset jobs")

In this section, we'll demonstrate how to create a few asset jobs that target the following assets:

```codeBlockLines_e6Vv
@dg.asset
def sugary_cereals() -> None:
    execute_query(
        "CREATE TABLE sugary_cereals AS SELECT * FROM cereals WHERE sugar_grams > 10"
    )

@dg.asset(deps=[sugary_cereals])
def shopping_list() -> None:
    execute_query("CREATE TABLE shopping_list AS SELECT * FROM sugary_cereals")

```

To create an asset job, use the [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) method. An asset-based job is based on the assets the job targets and their dependencies.

You can target one or multiple assets, or create multiple jobs that target overlapping sets of assets. In the following example, we have two jobs:

- `all_assets_job` targets all assets
- `sugary_cereals_job` targets only the `sugary_cereals` asset

```codeBlockLines_e6Vv
all_assets_job = dg.define_asset_job(name="all_assets_job")

sugary_cereals_job = dg.define_asset_job(
    name="sugary_cereals_job", selection="sugary_cereals"
)

defs = dg.Definitions(
    assets=[sugary_cereals, shopping_list],
    jobs=[all_assets_job, sugary_cereals_job],
)

```

## Making asset jobs available to Dagster tools [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#making-asset-jobs-available-to-dagster-tools "Direct link to Making asset jobs available to Dagster tools")

Including the jobs in a [`Definitions`](https://docs.dagster.io/api/dagster/definitions) object located at the top level of a Python module or file makes asset jobs available to the UI, GraphQL, and the command line. The Dagster tool loads that module as a code location. If you include schedules or sensors, the [code location](https://docs.dagster.io/guides/deploy/code-locations) will automatically include jobs that those schedules or sensors target.

```codeBlockLines_e6Vv
from dagster import Definitions, MaterializeResult, asset, define_asset_job

@asset
def number_asset():
    yield MaterializeResult(
        metadata={
            "number": 1,
        }
    )

number_asset_job = define_asset_job(name="number_asset_job", selection="number_asset")

defs = Definitions(
    assets=[number_asset],
    jobs=[number_asset_job],
)

```

## Testing asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#testing-asset-jobs "Direct link to Testing asset jobs")

Dagster has built-in support for testing, including separating business logic from environments and setting explicit expectations on uncontrollable inputs. For more information, see the [testing documentation](https://docs.dagster.io/guides/test).

## Executing asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#executing-asset-jobs "Direct link to Executing asset jobs")

You can run an asset job in a variety of ways:

- In the Python process where it's defined
- Via the command line
- Via the GraphQL API
- In the UI

## Examples [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#examples "Direct link to Examples")

The [Hacker News example](https://github.com/dagster-io/dagster/tree/master/examples/project_fully_featured) [builds an asset job that targets an asset group](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/jobs.py).

- [Creating asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#creating-asset-jobs)
- [Making asset jobs available to Dagster tools](https://docs.dagster.io/guides/build/jobs/asset-jobs#making-asset-jobs-available-to-dagster-tools)
- [Testing asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#testing-asset-jobs)
- [Executing asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#executing-asset-jobs)
- [Examples](https://docs.dagster.io/guides/build/jobs/asset-jobs#examples)
