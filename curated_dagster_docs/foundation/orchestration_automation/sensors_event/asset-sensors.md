
On this page

Asset sensors in Dagster provide a powerful mechanism for monitoring asset materializations and triggering downstream computations or notifications based on those events.

This guide covers the most common use cases for asset sensors, such as defining cross-job and cross-code location dependencies.

note

This documentation assumes familiarity with [assets](https://docs.dagster.io/guides/build/assets/) and [jobs](https://docs.dagster.io/guides/build/jobs/).

## Getting started [​](https://docs.dagster.io/guides/automate/asset-sensors\#getting-started "Direct link to Getting started")

Asset sensors monitor an asset for new materialization events and target a job when a new materialization occurs.

Typically, asset sensors return a `RunRequest` when a new job is to be triggered. However, they may provide a `SkipReason` if the asset materialization doesn't trigger a job.

For example, you may wish to monitor an asset that's materialized daily, but don't want to trigger jobs on holidays.

## Cross-job and cross-code location dependencies [​](https://docs.dagster.io/guides/automate/asset-sensors\#cross-job-and-cross-code-location-dependencies "Direct link to Cross-job and cross-code location dependencies")

Asset sensors enable dependencies across different jobs and different code locations. This flexibility allows for modular and decoupled workflows.

CodeLocationB

CodeLocationA

AssetToWatch

AssetSensor

Job

Asset1

Asset1

This is an example of an asset sensor that triggers a job when an asset is materialized. The `daily_sales_data` asset is in the same code location as the job and other asset for this example, but the same pattern can be applied to assets in different code locations.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def daily_sales_data(context: dg.AssetExecutionContext):
    context.log.info("Asset to watch")

@dg.asset
def weekly_report(context: dg.AssetExecutionContext):
    context.log.info("Asset to trigger")

# Job that materializes the `weekly_report` asset
my_job = dg.define_asset_job("my_job", [weekly_report])

# Trigger `my_job` when the `daily_sales_data` asset is materialized
@dg.asset_sensor(asset_key=dg.AssetKey("daily_sales_data"), job_name="my_job")
def daily_sales_data_sensor():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[daily_sales_data, weekly_report],
    jobs=[my_job],
    sensors=[daily_sales_data_sensor],
)

```

## Customizing the evaluation function of an asset sensor [​](https://docs.dagster.io/guides/automate/asset-sensors\#customizing-the-evaluation-function-of-an-asset-sensor "Direct link to Customizing the evaluation function of an asset sensor")

You can customize the evaluation function of an asset sensor to include specific logic for deciding when to trigger a run. This allows for fine-grained control over the conditions under which downstream jobs are executed.

AssetMaterialization

User Evaluation Function

RunRequest

SkipReason

In the following example, the `@asset_sensor` decorator defines a custom evaluation function that returns a `RunRequest` object when the asset is materialized and certain metadata is present, otherwise it skips the run.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def daily_sales_data(context: dg.AssetExecutionContext):
    context.log.info("Asset to watch, perhaps some function sets metadata here")
    yield dg.MaterializeResult(metadata={"specific_property": "value"})

@dg.asset
def weekly_report(context: dg.AssetExecutionContext):
    context.log.info("Running weekly report")

my_job = dg.define_asset_job("my_job", [weekly_report])

@dg.asset_sensor(asset_key=dg.AssetKey("daily_sales_data"), job=my_job)
def daily_sales_data_sensor(context: dg.SensorEvaluationContext, asset_event):
    # Provide a type hint on the underlying event
    materialization: dg.AssetMaterialization = (
        asset_event.dagster_event.event_specific_data.materialization
    )

    # Example custom logic: Check if the asset metadata has a specific property
    if "specific_property" in materialization.metadata:
        context.log.info("Triggering job based on custom evaluation logic")
        yield dg.RunRequest(run_key=context.cursor)
    else:
        yield dg.SkipReason("Asset materialization does not have the required property")

defs = dg.Definitions(
    assets=[daily_sales_data, weekly_report],
    jobs=[my_job],
    sensors=[daily_sales_data_sensor],
)

```

## Triggering a job with custom configuration [​](https://docs.dagster.io/guides/automate/asset-sensors\#triggering-a-job-with-custom-configuration "Direct link to Triggering a job with custom configuration")

By providing a configuration to the `RunRequest` object, you can trigger a job with a specific configuration. This is useful when you want to trigger a job with custom parameters based on custom logic you define.

For example, you might use a sensor to trigger a job when an asset is materialized, but also pass metadata about that materialization to the job:

```codeBlockLines_e6Vv
import dagster as dg

class MyConfig(dg.Config):
    param1: str

@dg.asset
def daily_sales_data(context: dg.AssetExecutionContext):
    context.log.info("Asset to watch")
    # Materialization metadata
    yield dg.MaterializeResult(metadata={"specific_property": "value"})

@dg.asset
def weekly_report(context: dg.AssetExecutionContext, config: MyConfig):
    context.log.info(f"Running weekly report with param1: {config.param1}")

my_job = dg.define_asset_job(
    "my_job",
    [weekly_report],
    config=dg.RunConfig(ops={"weekly_report": MyConfig(param1="value")}),
)

@dg.asset_sensor(asset_key=dg.AssetKey("daily_sales_data"), job=my_job)
def daily_sales_data_sensor(context: dg.SensorEvaluationContext, asset_event):
    materialization: dg.AssetMaterialization = (
        asset_event.dagster_event.event_specific_data.materialization
    )

    # Custom logic that checks if the asset metadata has a specific property
    if "specific_property" in materialization.metadata:
        yield dg.RunRequest(
            run_key=context.cursor,
            run_config=dg.RunConfig(
                ops={
                    "weekly_report": MyConfig(
                        param1=str(materialization.metadata.get("specific_property"))
                    )
                }
            ),
        )

defs = dg.Definitions(
    assets=[daily_sales_data, weekly_report],
    jobs=[my_job],
    sensors=[daily_sales_data_sensor],
)

```

## Monitoring multiple assets [​](https://docs.dagster.io/guides/automate/asset-sensors\#monitoring-multiple-assets "Direct link to Monitoring multiple assets")

note

The `@multi_asset_sensor` has been marked as deprecated, but will not be removed from the codebase until Dagster 2.0 is released, meaning it will continue to function as it currently does for the foreseeable future. Its functionality has been largely superseded by the `AutomationCondition` system. For more information, see the [Declarative Automation documentation](https://docs.dagster.io/guides/automate/declarative-automation/).

When building a pipeline, you may want to monitor multiple assets with a single sensor. This can be accomplished with a multi-asset sensor.

The following example uses a `@multi_asset_sensor` to monitor two assets that triggers an asset job once both have been materialized. You can also trigger op jobs this way.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def asset_a():
    return [1, 2, 3]

@dg.asset
def asset_b():
    return [5, 6, 7]

@dg.asset
def asset_c():
    return [8, 9, 10]

asset_job = dg.define_asset_job(
    "asset_c_job",
    selection=[dg.AssetKey("asset_c")],
)

@dg.multi_asset_sensor(
    monitored_assets=[dg.AssetKey("asset_a"), dg.AssetKey("asset_b")],
    job=asset_job,
)
def asset_a_and_b_sensor(context):
    asset_events = context.latest_materialization_records_by_key()
    if all(asset_events.values()):
        context.advance_all_cursors()
        return dg.RunRequest(run_key=context.cursor, run_config={})
    return None

defs = dg.Definitions(
    assets=[asset_a, asset_b, asset_c], jobs=[asset_job], sensors=[asset_a_and_b_sensor]
)

```

## Next steps [​](https://docs.dagster.io/guides/automate/asset-sensors\#next-steps "Direct link to Next steps")

- Explore [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation/) as an alternative to asset sensors

- [Getting started](https://docs.dagster.io/guides/automate/asset-sensors#getting-started)
- [Cross-job and cross-code location dependencies](https://docs.dagster.io/guides/automate/asset-sensors#cross-job-and-cross-code-location-dependencies)
- [Customizing the evaluation function of an asset sensor](https://docs.dagster.io/guides/automate/asset-sensors#customizing-the-evaluation-function-of-an-asset-sensor)
- [Triggering a job with custom configuration](https://docs.dagster.io/guides/automate/asset-sensors#triggering-a-job-with-custom-configuration)
- [Monitoring multiple assets](https://docs.dagster.io/guides/automate/asset-sensors#monitoring-multiple-assets)
- [Next steps](https://docs.dagster.io/guides/automate/asset-sensors#next-steps)
