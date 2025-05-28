
On this page

While it is most common to use automation conditions to automate the execution of assets, you can also use and customize them in the same ways to automate the execution of asset checks. This is particularly helpful in cases where you want to automate your data quality checks independently of the assets they are associated with.

## Recommended automation conditions [​](https://docs.dagster.io/guides/automate/declarative-automation/automating-asset-checks\#recommended-automation-conditions "Direct link to Recommended automation conditions")

- on\_cron
- eager

The [`AutomationCondition.on_cron`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.on_cron) will execute an asset check once per cron schedule tick, after all upstream dependencies have updated. This allows you to check the quality of your data on a lower frequency than the asset itself updates, which can be helpful in cases where the data quality check is slow or expensive to execute.

In the following example, at the start of each hour, the above check will start waiting for its associated asset to be updated. Once this happens, the check will immediately be requested. The check will not be requested again until the next hour.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset_check(
    asset="upstream",
    automation_condition=dg.AutomationCondition.on_cron("@hourly"),
)
def on_cron_asset_check() -> dg.AssetCheckResult: ...

```

**Behavior**

- If at least one upstream partition of _all_ upstream assets has been updated since the previous cron schedule tick, and the downstream check has not yet been requested or executed, it will be requested.
- If all upstream assets **do not** update within the given cron tick, the check will not be requested.

If you would like to customize aspects of this behavior, refer to the [customizing on\_cron](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/customizing-on-cron-condition) guide.

The [`AutomationCondition.eager`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.eager) condition allows you to automatically execute an asset check whenever any of its dependencies are updated. This is used to ensure that whenever upstream changes happen, they are automatically checked for data quality issues.

In the following example, the asset check will be automatically requested whenever any of its upstream dependencies are updated.

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset_check(
    asset="upstream",
    automation_condition=dg.AutomationCondition.eager(),
)
def eager_asset_check() -> dg.AssetCheckResult: ...

```

**Behavior**

If you would like to customize aspects of this behavior, refer to the [customizing eager](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/customizing-eager-condition) guide.

- If _any_ upstream partitions have not been materialized, the downstream check will not be requested.
- If _any_ upstream partitions are currently part of an in-progress run, the downstream check will wait for those runs to complete before being requested.
- If the asset check is already part of an in-progress run, it will wait for that run to complete before being requested.
- If an upstream asset is _observed_, this will only be treated as an update to the upstream asset if the data version has changed since the previous observation.
- If an upstream asset is _materialized_, this will be treated as an update to the upstream asset regardless of the data version of that materialization.

- [Recommended automation conditions](https://docs.dagster.io/guides/automate/declarative-automation/automating-asset-checks#recommended-automation-conditions)
