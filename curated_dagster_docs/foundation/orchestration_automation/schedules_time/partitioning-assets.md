
On this page

In Dagster, partitioning is a powerful technique for managing large datasets, improving pipeline performance, and enabling incremental processing. This guide will help you understand how to implement data partitioning in your Dagster projects.

There are several ways to partition your data in Dagster:

- [Time-based partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#time-based), for processing data in specific time intervals
- [Static partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#static-partitions), for dividing data based on predefined categories
- [Two-dimensional partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#two-dimensional-partitions), for partitioning data along two different axes simultaneously
- [Dynamic partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#dynamic-partitions), for creating partitions based on runtime information

note

We recommend limiting the number of partitions for each asset to 25,000 or fewer. Assets with partition counts exceeding this limit will likely have slower load times in the UI.

## Time-based partitions [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#time-based "Direct link to Time-based partitions")

A common use case for partitioning is to process data that can be divided into time intervals, such as daily logs or monthly reports.

```codeBlockLines_e6Vv
import datetime
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition,
# which will create a range of partitions from
# 2024-01-01 to the day before the current time
daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")

# Define the partitioned asset
@dg.asset(partitions_def=daily_partitions)
def daily_sales_data(context: dg.AssetExecutionContext) -> None:
    date = context.partition_key
    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "date": [date] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/daily_sales", exist_ok=True)
    filename = f"data/daily_sales/sales_{date}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Daily sales data written to {filename}")

@dg.asset(
    partitions_def=daily_partitions,  # Use the daily partitioning scheme
    deps=[daily_sales_data],  # Define dependency on `daily_sales_data` asset
)
def daily_sales_summary(context):
    partition_date_str = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/daily_sales/sales_{partition_date_str}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "date": partition_date_str,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Daily sales summary for {partition_date_str}: {summary}")

# Create a partitioned asset job
daily_sales_job = dg.define_asset_job(
    name="daily_sales_job",
    selection=[daily_sales_data, daily_sales_summary],
)

# Create a schedule to run the job daily
@dg.schedule(
    job=daily_sales_job,
    cron_schedule="0 1 * * *",  # Run at 1:00 AM every day
)
def daily_sales_schedule(context):
    """Process previous day's sales data."""
    # Calculate the previous day's date
    previous_day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    date = previous_day.strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=date,
        partition_key=date,
    )

# Define the Definitions object
defs = dg.Definitions(
    assets=[daily_sales_data, daily_sales_summary],
    jobs=[daily_sales_job],
    schedules=[daily_sales_schedule],
)

```

## Partitions with predefined categories [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#static-partitions "Direct link to Partitions with predefined categories")

Sometimes you have a set of predefined categories for your data. For instance, you might want to process data separately for different regions.

```codeBlockLines_e6Vv
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])

# Define the partitioned asset
@dg.asset(partitions_def=region_partitions)  # Use the region partitioning scheme
def regional_sales_data(context: dg.AssetExecutionContext) -> None:
    region = context.partition_key

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/regional_sales", exist_ok=True)
    filename = f"data/regional_sales/sales_{region}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Regional sales data written to {filename}")

@dg.asset(
    partitions_def=region_partitions,  # Use the region partitioning scheme
    deps=[regional_sales_data],
)
def daily_sales_summary(context):
    region = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/regional_sales/sales_{region}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Regional sales summary for {region}: {summary}")

# Create a partitioned asset job
regional_sales_job = dg.define_asset_job(
    name="regional_sales_job",
    selection=[regional_sales_data, daily_sales_summary],
)

# Define the Definitions object
defs = dg.Definitions(
    assets=[regional_sales_data, daily_sales_summary],
    jobs=[regional_sales_job],
)

```

## Two-dimensional partitions [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#two-dimensional-partitions "Direct link to Two-dimensional partitions")

Two-dimensional partitioning allows you to partition data along two different axes simultaneously. This is useful when you need to process data that can be categorized in multiple ways. For example:

```codeBlockLines_e6Vv
import datetime
import os

import pandas as pd

import dagster as dg

# Create two PartitionDefinitions
daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])
two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": daily_partitions, "region": region_partitions}
)

# Define the partitioned asset
@dg.asset(partitions_def=two_dimensional_partitions)
def daily_regional_sales_data(context: dg.AssetExecutionContext) -> None:
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "date": [date] * 10,
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/daily_regional_sales", exist_ok=True)
    filename = f"data/daily_regional_sales/sales_{context.partition_key}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Daily sales data written to {filename}")

@dg.asset(
    partitions_def=two_dimensional_partitions,
    deps=[daily_regional_sales_data],
)
def daily_regional_sales_summary(context):
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]

    filename = f"data/daily_regional_sales/sales_{context.partition_key}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "date": date,
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Daily sales summary for {context.partition_key}: {summary}")

# Create a partitioned asset job
daily_regional_sales_job = dg.define_asset_job(
    name="daily_regional_sales_job",
    selection=[daily_regional_sales_data, daily_regional_sales_summary],
)

# Create a schedule to run the job daily
@dg.schedule(
    job=daily_regional_sales_job,
    cron_schedule="0 1 * * *",  # Run at 1:00 AM every day
)
def daily_regional_sales_schedule(context):
    """Process previous day's sales data for all regions."""
    previous_day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    date = previous_day.strftime("%Y-%m-%d")

    # Create a run request for each region (3 runs in total every day)
    return [\
        dg.RunRequest(\
            run_key=f"{date}|{region}",\
            partition_key=dg.MultiPartitionKey({"date": date, "region": region}),\
        )\
        for region in region_partitions.get_partition_keys()\
    ]

# Define the Definitions object
defs = dg.Definitions(
    assets=[daily_regional_sales_data, daily_regional_sales_summary],
    jobs=[daily_regional_sales_job],
    schedules=[daily_regional_sales_schedule],
)

```

In this example:

- Using `MultiPartitionsDefinition`, the `two_dimensional_partitions` is defined with two dimensions: `date` and `region`
- The partition key would be: `2024-08-01|us`
- The `daily_regional_sales_data` and `daily_regional_sales_summary` assets are defined with the same two-dimensional partitioning scheme
- The `daily_regional_sales_schedule` runs daily at 1:00 AM, processing the previous day's data for all regions. It uses `MultiPartitionKey` to specify partition keys for both date and region dimensions, resulting in three runs per day, one for each region.

## Partitions with dynamic categories [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#dynamic-partitions "Direct link to Partitions with dynamic categories")

Sometimes you don't know the partitions in advance. For example, you might want to process new regions that are added in your system. In these cases, you can use dynamic partitioning to create partitions based on runtime information.

Consider this example:

Dynamic partitioning

```codeBlockLines_e6Vv
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition
region_partitions = dg.DynamicPartitionsDefinition(name="regions")

# Define the partitioned asset
@dg.asset(partitions_def=region_partitions)
def regional_sales_data(context: dg.AssetExecutionContext) -> None:
    region = context.partition_key

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/regional_sales", exist_ok=True)
    filename = f"data/regional_sales/sales_{region}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Regional sales data written to {filename}")

@dg.asset(
    partitions_def=region_partitions,
    deps=[regional_sales_data],
)
def daily_sales_summary(context):
    region = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/regional_sales/sales_{region}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Regional sales summary for {region}: {summary}")

# Create a partitioned asset job
regional_sales_job = dg.define_asset_job(
    name="regional_sales_job",
    selection=[regional_sales_data, daily_sales_summary],
)

@dg.sensor(job=regional_sales_job)
def all_regions_sensor(context: dg.SensorEvaluationContext):
    # Simulate fetching all regions from an external system
    all_regions = ["us", "eu", "jp", "ca", "uk", "au"]

    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=region) for region in all_regions],
        dynamic_partitions_requests=[region_partitions.build_add_request(all_regions)],
    )

# Define the Definitions object
defs = dg.Definitions(
    assets=[regional_sales_data, daily_sales_summary],
    jobs=[regional_sales_job],
    sensors=[all_regions_sensor],
)

```

In this example:

- Because the partition values are unknown in advance, `DynamicPartitionsDefinition` is used to define `region_partitions`
- When triggered, the `all_regions_sensor` will dynamically add all regions to the partition set. Once it kicks off runs, it will dynamically kick off runs for all regions. In this example, that would be six times; one for each region.

## Materializing partitioned assets [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#materializing-partitioned-assets "Direct link to Materializing partitioned assets")

When you materialize a partitioned asset, you choose which partitions to materialize and Dagster will launch a run for each partition.

note

If you choose more than one partition, the [Dagster daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) needs to be running to queue the multiple runs.

The following image shows the **Launch runs** dialog on an asset's **Details** page, where you'll be prompted to select a partition to materialize:

![Rematerialize partition](https://docs.dagster.io/assets/images/rematerialize-partition-a57c886d22ee57191eff6e1a794af7e2.png)

After a partition has been successfully materialized, it will display as green in the partitions bar:

![Successfully materialized partition](https://docs.dagster.io/assets/images/materialized-partitioned-asset-360546ca8f4a4a845de9c27c9b8562fa.png)

## Viewing materializations by partition [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#viewing-materializations-by-partition "Direct link to Viewing materializations by partition")

To view materializations by partition for a specific asset, navigate to the **Activity** tab of the asset's **Details** page:

![Asset activity section of asset details page](https://docs.dagster.io/assets/images/materialized-partitioned-asset-activity-5269cab8c25188337b6f56af25cd906d.png)

- [Time-based partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#time-based)
- [Partitions with predefined categories](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#static-partitions)
- [Two-dimensional partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#two-dimensional-partitions)
- [Partitions with dynamic categories](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#dynamic-partitions)
- [Materializing partitioned assets](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#materializing-partitioned-assets)
- [Viewing materializations by partition](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#viewing-materializations-by-partition)
