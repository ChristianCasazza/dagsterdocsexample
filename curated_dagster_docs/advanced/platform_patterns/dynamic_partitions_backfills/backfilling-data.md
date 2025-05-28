
On this page

Backfilling is the process of running partitions for assets that either don't exist or updating existing records. Dagster supports backfills for each partition or a subset of partitions.

After defining a [partition](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets), you can launch a backfill that will submit runs to fill in multiple partitions at the same time.

Backfills are common when setting up a pipeline for the first time. The assets you want to materialize might have historical data that needs to be materialized to get the assets up to date. Another common reason to run a backfill is when you've changed the logic for an asset and need to update historical data with the new logic.

## Launching backfills for partitioned assets [​](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data\#launching-backfills-for-partitioned-assets "Direct link to Launching backfills for partitioned assets")

To launch backfills for a partitioned asset, click the **Materialize** button on either the **Asset details** or the **Global asset lineage** page. The backfill modal will display.

Backfills can also be launched for a selection of partitioned assets as long as the most upstream assets share the same partitioning. For example: All partitions use a `DailyPartitionsDefinition`.

![Backfills launch modal](https://docs.dagster.io/assets/images/asset-backfill-partition-selection-modal-42cc5b2620aec95c8e22046e7f7a4340.png)

To observe the progress of an asset backfill, navigate to the **Runs details** page for the run. This page can be accessed by clicking **Runs tab**, then clicking the ID of the run. To see all runs, including runs launched by a backfill, check the **Show runs within backfills** box:

![Asset backfill details page](https://docs.dagster.io/assets/images/asset-backfill-details-page-12261047ca52b602ab8244b0ebb41819.png)

## Launching single-run backfills using backfill policies [​](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data\#launching-single-run-backfills-using-backfill-policies "Direct link to Launching single-run backfills using backfill policies")

By default, if you launch a backfill that covers `N` partitions, Dagster will launch `N` separate runs, one for each partition. This approach can help avoid overwhelming Dagster or resources with large amounts of data. However, if you're using a parallel-processing engine like Spark and Snowflake, you often don't need Dagster to help with parallelism, so splitting up the backfill into multiple runs just adds extra overhead.

Dagster supports backfills that execute as a single run that covers a range of partitions, such as executing a backfill as a single Snowflake query. After the run completes, Dagster will track that all the partitions have been filled.

note

Single-run backfills only work if they are launched from the asset graph or
asset page, or if the assets are part of an asset job that shares the same
backfill policy across all included assets.

To get this behavior, you need to:

- **Set the asset's `backfill_policy` ( [`BackfillPolicy`](https://docs.dagster.io/api/dagster/partitions#dagster.BackfillPolicy))** to `single_run`

- **Write code that operates on a range of partitions** instead of just single partitions. This means that, if your code uses the `partition_key` context property, you'll need to update it to use one of the following properties instead:


  - [`partition_time_window`](https://docs.dagster.io/api/dagster/execution#dagster.OpExecutionContext.partition_time_window)
  - [`partition_key_range`](https://docs.dagster.io/api/dagster/execution#dagster.OpExecutionContext.partition_key_range)
  - [`partition_keys`](https://docs.dagster.io/api/dagster/execution#dagster.OpExecutionContext.partition_keys)

Which property to use depends on whether it's most convenient for you to operate on start/end datetime objects, start/end partition keys, or a list of partition keys.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    partitions_def=dg.DailyPartitionsDefinition(start_date="2020-01-01"),
    backfill_policy=dg.BackfillPolicy.single_run(),
    deps=[dg.AssetKey("raw_events")],
)
def events(context: dg.AssetExecutionContext) -> None:
    start_datetime, end_datetime = context.partition_time_window

    input_data = read_data_in_datetime_range(start_datetime, end_datetime)
    output_data = compute_events_from_raw_events(input_data)

    overwrite_data_in_datetime_range(start_datetime, end_datetime, output_data)

```

- [Launching backfills for partitioned assets](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data#launching-backfills-for-partitioned-assets)
- [Launching single-run backfills using backfill policies](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data#launching-single-run-backfills-using-backfill-policies)
