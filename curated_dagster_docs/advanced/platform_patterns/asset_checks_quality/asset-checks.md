
On this page

Asset checks are tests that verify specific properties of your data assets, allowing you to execute data quality checks on your data. For example, you can create checks to:

- Ensure a particular column doesn't contain null values
- Verify that a tabular asset adheres to a specified schema
- Check if an asset's data needs refreshing

Each asset check should test only a single asset property to keep tests uncomplicated, reusable, and easy to track over time.

note

This article assumes familiarity with [assets](https://docs.dagster.io/guides/build/assets/).

## Getting started [​](https://docs.dagster.io/guides/test/asset-checks\#getting-started "Direct link to Getting started")

To get started with asset checks, follow these general steps:

1. **Define an asset check:** Asset checks are typically defined using the `@asset_check` or `@multi_asset_check` decorator and run either within an asset or separate from the asset.
2. **Pass the asset checks to the `Definitions` object:** Asset checks must be added to `Definitions` for Dagster to recognize them.
3. **Choose how to execute asset checks**: By default, all jobs targeting an asset will also run associated checks, although you can run asset checks through the Dagster UI.
4. **View asset check results in the UI**: Asset check results will appear in the UI and can be customized through the use of metadata and severity levels
5. **Alert on failed asset check results**: If you are using Dagster+, you can choose to alert on asset checks.

## Defining a single asset check [​](https://docs.dagster.io/guides/test/asset-checks\#single-check "Direct link to Defining a single asset check")

tip

Dagster's dbt integration can model existing dbt tests as asset checks. Refer to the [dagster-dbt documentation](https://docs.dagster.io/integrations/libraries/dbt) for more information.

A asset check is defined using the `@asset_check` decorator.

The following example defines an asset check on an asset that fails if the `order_id` column of the asset contains a null value. The asset check will run after the asset has been materialized.

```codeBlockLines_e6Vv
import pandas as pd

import dagster as dg

@dg.asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")

@dg.asset_check(asset=orders)
def orders_id_has_no_nulls():
    orders_df = pd.read_csv("orders.csv")
    num_null_order_ids = orders_df["order_id"].isna().sum()

    # Return the result of the check
    return dg.AssetCheckResult(
        # Define passing criteria
        passed=bool(num_null_order_ids == 0),
    )

defs = dg.Definitions(
    assets=[orders],
    asset_checks=[orders_id_has_no_nulls],
)

```

## Defining multiple asset checks [​](https://docs.dagster.io/guides/test/asset-checks\#multiple-checks "Direct link to Defining multiple asset checks")

In most cases, checking the data quality of an asset will require multiple checks.

The following example defines two asset checks using the `@multi_asset_check` decorator:

- One check that fails if the `order_id` column of the asset contains a null value
- Another check that fails if the `item_id` column of the asset contains a null value

In this example, both asset checks will run in a single operation after the asset has been materialized.

```codeBlockLines_e6Vv
from collections.abc import Iterable

import pandas as pd

import dagster as dg

@dg.asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")

@dg.multi_asset_check(
    # Map checks to targeted assets
    specs=[\
        dg.AssetCheckSpec(name="orders_id_has_no_nulls", asset="orders"),\
        dg.AssetCheckSpec(name="items_id_has_no_nulls", asset="orders"),\
    ]
)
def orders_check() -> Iterable[dg.AssetCheckResult]:
    orders_df = pd.read_csv("orders.csv")

    # Check for null order_id column values
    num_null_order_ids = orders_df["order_id"].isna().sum()
    yield dg.AssetCheckResult(
        check_name="orders_id_has_no_nulls",
        passed=bool(num_null_order_ids == 0),
        asset_key="orders",
    )

    # Check for null item_id column values
    num_null_item_ids = orders_df["item_id"].isna().sum()
    yield dg.AssetCheckResult(
        check_name="items_id_has_no_nulls",
        passed=bool(num_null_item_ids == 0),
        asset_key="orders",
    )

defs = dg.Definitions(
    assets=[orders],
    asset_checks=[orders_check],
)

```

## Programmatically generating asset checks [​](https://docs.dagster.io/guides/test/asset-checks\#factory-pattern "Direct link to Programmatically generating asset checks")

Defining multiple checks can also be done using a factory pattern. The example below defines the same two asset checks as in the previous example, but this time using a factory pattern and the `@multi_asset_check` decorator.

```codeBlockLines_e6Vv
from collections.abc import Iterable, Mapping, Sequence

import pandas as pd

import dagster as dg

@dg.asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")

def make_orders_checks(
    check_blobs: Sequence[Mapping[str, str]],
) -> dg.AssetChecksDefinition:
    @dg.multi_asset_check(
        specs=[\
            dg.AssetCheckSpec(name=check_blob["name"], asset=check_blob["asset"])\
            for check_blob in check_blobs\
        ]
    )
    def orders_check() -> Iterable[dg.AssetCheckResult]:
        orders_df = pd.read_csv("orders.csv")

        for check_blob in check_blobs:
            num_null_order_ids = orders_df[check_blob["column"]].isna().sum()
            yield dg.AssetCheckResult(
                check_name=check_blob["name"],
                passed=bool(num_null_order_ids == 0),
                asset_key=check_blob["asset"],
            )

    return orders_check

check_blobs = [\
    {\
        "name": "orders_id_has_no_nulls",\
        "asset": "orders",\
        "column": "order_id",\
    },\
    {\
        "name": "items_id_has_no_nulls",\
        "asset": "orders",\
        "column": "item_id",\
    },\
]

defs = dg.Definitions(
    assets=[orders],
    asset_checks=[make_orders_checks(check_blobs)],
)

```

## Blocking downstream materialization [​](https://docs.dagster.io/guides/test/asset-checks\#blocking-downstream-materialization "Direct link to Blocking downstream materialization")

By default, if a parent's asset check fails during a run, the run will continue and downstream assets will be materialized. To prevent this behavior, set the `blocking` argument to `True` in the `@asset_check` decorator.

In the example bellow, if the `orders_id_has_no_nulls` check fails, the downstream `augmented_orders` asset won't be materialized.

```codeBlockLines_e6Vv
import pandas as pd

import dagster as dg

@dg.asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")

# Check that targets `orders`; block materialization of `augmented_orders` on failure
@dg.asset_check(asset=orders, blocking=True)
def orders_id_has_no_nulls():
    orders_df = pd.read_csv("orders.csv")
    num_null_order_ids = orders_df["order_id"].isna().sum()
    return dg.AssetCheckResult(
        passed=bool(num_null_order_ids == 0),
    )

# Asset downstream of `orders`
@dg.asset(deps=[orders])
def augmented_orders():
    orders_df = pd.read_csv("orders.csv")
    augmented_orders_df = orders_df.assign(description=["item_432", "item_878"])
    augmented_orders_df.to_csv("augmented_orders.csv")

defs = dg.Definitions(
    assets=[orders, augmented_orders],
    asset_checks=[orders_id_has_no_nulls],
)

```

## Scheduling and monitoring asset checks [​](https://docs.dagster.io/guides/test/asset-checks\#scheduling-and-monitoring-asset-checks "Direct link to Scheduling and monitoring asset checks")

In some cases, running asset checks separately from the job materializing the assets can be useful. For example, running all data quality checks once a day and sending an alert if they fail. This can be achieved using schedules and sensors.

In the example below, two jobs are defined: one for the asset and another for the asset check. Schedules are defined to materialize the asset and execute the asset check independently. A sensor is defined to send an email alert when the asset check job fails.

```codeBlockLines_e6Vv
import os

import pandas as pd

import dagster as dg

@dg.asset
def orders():
    orders_df = pd.DataFrame({"order_id": [1, 2], "item_id": [432, 878]})
    orders_df.to_csv("orders.csv")

@dg.asset_check(asset=orders)
def orders_id_has_no_nulls():
    orders_df = pd.read_csv("orders.csv")
    num_null_order_ids = orders_df["order_id"].isna().sum()
    return dg.AssetCheckResult(
        passed=bool(num_null_order_ids == 0),
    )

# Only include the `orders` asset
asset_job = dg.define_asset_job(
    "asset_job",
    selection=dg.AssetSelection.assets(orders).without_checks(),
)

# Only include the `orders_id_has_no_nulls` check
check_job = dg.define_asset_job(
    "check_job", selection=dg.AssetSelection.checks_for_assets(orders)
)

# Job schedules
asset_schedule = dg.ScheduleDefinition(job=asset_job, cron_schedule="0 0 * * *")
check_schedule = dg.ScheduleDefinition(job=check_job, cron_schedule="0 6 * * *")

# Send email on failure
check_sensor = dg.make_email_on_run_failure_sensor(
    email_from="no-reply@example.com",
    email_password=os.getenv("ALERT_EMAIL_PASSWORD"),
    email_to=["xxx@example.com"],
    monitored_jobs=[check_job],
)

defs = dg.Definitions(
    assets=[orders],
    asset_checks=[orders_id_has_no_nulls],
    jobs=[asset_job, check_job],
    schedules=[asset_schedule, check_schedule],
    sensors=[check_sensor],
)

```

## Next steps [​](https://docs.dagster.io/guides/test/asset-checks\#next-steps "Direct link to Next steps")

- Learn more about [assets](https://docs.dagster.io/guides/build/assets/)
- Learn how to use [Great Expectations with Dagster](https://dagster.io/blog/ensuring-data-quality-with-dagster-and-great-expectations)

- [Getting started](https://docs.dagster.io/guides/test/asset-checks#getting-started)
- [Defining a single asset check](https://docs.dagster.io/guides/test/asset-checks#single-check)
- [Defining multiple asset checks](https://docs.dagster.io/guides/test/asset-checks#multiple-checks)
- [Programmatically generating asset checks](https://docs.dagster.io/guides/test/asset-checks#factory-pattern)
- [Blocking downstream materialization](https://docs.dagster.io/guides/test/asset-checks#blocking-downstream-materialization)
- [Scheduling and monitoring asset checks](https://docs.dagster.io/guides/test/asset-checks#scheduling-and-monitoring-asset-checks)
- [Next steps](https://docs.dagster.io/guides/test/asset-checks#next-steps)
