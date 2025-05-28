
On this page

You often want to control the number of concurrent runs for a Dagster job, a specific asset, or for a type of asset or job. Limiting concurrency in your data pipelines can help prevent performance problems and downtime.

note

This article assumes familiarity with [assets](https://docs.dagster.io/guides/build/assets/) and [jobs](https://docs.dagster.io/guides/build/jobs/).

## Limit the number of total runs that can be in progress at the same time [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-total-runs-that-can-be-in-progress-at-the-same-time "Direct link to Limit the number of total runs that can be in progress at the same time")

- Dagster Core, add the following to your [dagster.yaml](https://docs.dagster.io/guides/deploy/dagster-yaml)
- In Dagster+, add the following to your [deployment settings](https://docs.dagster.io/dagster-plus/deployment/management/deployments/deployment-settings-reference)

```codeBlockLines_e6Vv
concurrency:
  runs:
    max_concurrent_runs: 15

```

## Limit the number of assets or ops actively executing across all runs [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-assets-or-ops-actively-executing-across-all-runs "Direct link to Limit the number of assets or ops actively executing across all runs")

You can assign assets and ops to concurrency pools which allow you to limit the number of in progress op executions across all runs. You first assign your asset or op to a concurrency pool using the `pool` keyword argument.

Specifying pools on assets and ops

```codeBlockLines_e6Vv
import time

import dagster as dg

@dg.asset(pool="foo")
def my_asset():
    pass

@dg.op(pool="bar")
def my_op():
    pass

@dg.op(pool="barbar")
def my_downstream_op(inp):
    return inp

@dg.graph_asset
def my_graph_asset():
    return my_downstream_op(my_op())

defs = dg.Definitions(
    assets=[my_asset, my_graph_asset],
)

```

You should be able to verify that you have set the pool correctly by viewing the details pane for the asset or op in the Dagster UI.

![Viewing the pool tag](https://docs.dagster.io/assets/images/asset-pool-tag-fd5900b211765e971bd379b0395edc67.png)

Once you have assigned your assets and ops to a concurrency pool, you can configure a pool limit for that pool in your deployment by using the Dagster UI or the Dagster CLI.

To specify a limit for the pool "database" using the UI, navigate to the `Deployments` → `Concurrency` settings page and click the `Add pool limit` button:

![Setting the pool limit](https://docs.dagster.io/assets/images/add-pool-ui-202927770b0febe9c7129b682a0ac3e6.png)

To specify a limit for the pool "database" using the CLI, use:

```codeBlockLines_e6Vv
dagster instance concurrency set database 1

```

## Limit the number of runs that can be in progress for a set of ops [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-runs-that-can-be-in-progress-for-a-set-of-ops "Direct link to Limit the number of runs that can be in progress for a set of ops")

You can also use concurrency pools to limit the number of in progress runs containing those assets or ops. You can follow the steps in the [Limit the number of assets or ops actively in execution across all runs](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-assets-or-ops-actively-executing-across-all-runs) section to assign your assets and ops to pools and to configure the desired limit.

Once you have assigned your assets and ops to your pool, you can change your deployment settings to set the pool enforcement granularity. To limit the total number of runs containing a specific op at any given time (instead of the total number of ops actively executing), we need to set the pool granularity to `run`.

- Dagster Core, add the following to your [dagster.yaml](https://docs.dagster.io/guides/deploy/dagster-yaml)
- In Dagster+, add the following to your [deployment settings](https://docs.dagster.io/dagster-plus/deployment/management/deployments/deployment-settings-reference)

```codeBlockLines_e6Vv
concurrency:
  pools:
    granularity: 'run'

```

Without this granularity set, the default granularity is set to the `op`. This means that for a pool `foo` with a limit `1`, we enforce that only one op is executing at a given time across all runs, but the number of runs in progress is unaffected by the pool limit.

### Setting a default limit for concurrency pools [​](https://docs.dagster.io/guides/operate/managing-concurrency\#setting-a-default-limit-for-concurrency-pools "Direct link to Setting a default limit for concurrency pools")

- Dagster+: Edit the `concurrency` config in deployment settings via the [Dagster+ UI](https://docs.dagster.io/guides/operate/webserver) or the [`dagster-cloud` CLI](https://docs.dagster.io/dagster-plus/deployment/management/dagster-cloud-cli/).
- Dagster Open Source: Use your instance's [dagster.yaml](https://docs.dagster.io/guides/deploy/dagster-yaml)

```codeBlockLines_e6Vv
concurrency:
  pools:
    default_limit: 1

```

## Limit the number of runs that can be in progress by run tag [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-runs-that-can-be-in-progress-by-run-tag "Direct link to Limit the number of runs that can be in progress by run tag")

You can also limit the number of in progress runs by run tag. This is useful for limiting sets of runs independent of which assets or ops it is executing. For example, you might want to limit the number of in-progress runs for a particular schedule. Or, you might want to limit the number of in-progress runs for all backfills.

```codeBlockLines_e6Vv
concurrency:
  runs:
    tag_concurrency_limits:
      - key: 'dagster/sensor_name'
        value: 'my_cool_sensor'
        limit: 5
      - key: 'dagster/backfill'
        limit: 10

```

### Limit the number of runs that can be in progress by unique tag value [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-runs-that-can-be-in-progress-by-unique-tag-value "Direct link to Limit the number of runs that can be in progress by unique tag value")

To apply separate limits to each unique value of a run tag, set a limit for each unique value using `applyLimitPerUniqueValue`. For example, instead of limiting the number of backfill runs across all backfills, you may want to limit the number of runs for each backfill in progress:

```codeBlockLines_e6Vv
concurrency:
  runs:
    tag_concurrency_limits:
      - key: 'dagster/backfill'
        value:
          applyLimitPerUniqueValue: true
        limit: 10

```

## Limit the number of ops concurrently executing for a single run [​](https://docs.dagster.io/guides/operate/managing-concurrency\#limit-the-number-of-ops-concurrently-executing-for-a-single-run "Direct link to Limit the number of ops concurrently executing for a single run")

While pool limits allow you to [limit the number of ops executing across all runs](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-assets-or-ops-actively-executing-across-all-runs), to limit the number of ops executing _within a single run_, you need to configure your [run executor](https://docs.dagster.io/guides/operate/run-executors). You can
limit concurrency for ops and assets in runs, by using `max_concurrent` in the run config, either in Python or using the Launchpad in the Dagster UI.

Limit concurrent op execution for a single run

```codeBlockLines_e6Vv
import time

import dagster as dg

@dg.asset
def first_asset(context: dg.AssetExecutionContext):
    time.sleep(75)
    context.log.info("first asset executing")

@dg.asset
def second_asset(context: dg.AssetExecutionContext):
    time.sleep(75)
    context.log.info("second asset executing")

@dg.asset
def third_asset(context: dg.AssetExecutionContext):
    time.sleep(75)
    context.log.info("third asset executing")

# limits concurrent asset execution for `my_job` runs to 2, overrides the limit set on the Definitions object
my_job = dg.define_asset_job(
    name="my_job",
    selection=[first_asset, second_asset, third_asset],
    executor_def=dg.multiprocess_executor.configured({"max_concurrent": 2}),
)

# limits concurrent asset execution for all runs in this code location to 4
defs = dg.Definitions(
    assets=[first_asset, second_asset, third_asset],
    jobs=[my_job],
    executor=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

```

The default limit for op execution within a run depends on which executor you are using. For example, the [`multiprocess_executor`](https://docs.dagster.io/api/dagster/execution#dagster.multiprocess_executor) by default limits the number of ops executing to the value of `multiprocessing.cpu_count()` in the launched run.

## Prevent runs from starting if another run is already occurring (advanced) [​](https://docs.dagster.io/guides/operate/managing-concurrency\#prevent-runs-from-starting-if-another-run-is-already-occurring-advanced "Direct link to Prevent runs from starting if another run is already occurring (advanced)")

You can use Dagster's rich metadata to use a schedule or a sensor to only start a run when there are no currently running jobs.

No more than 1 running job from a schedule

```codeBlockLines_e6Vv
import time

import dagster as dg

@dg.asset
def first_asset(context: dg.AssetExecutionContext):
    # sleep so that the asset takes some time to execute
    time.sleep(75)
    context.log.info("First asset executing")

my_job = dg.define_asset_job("my_job", [first_asset])

@dg.schedule(
    job=my_job,
    # Runs every minute to show the effect of the concurrency limit
    cron_schedule="* * * * *",
)
def my_schedule(context):
    # Find runs of the same job that are currently running
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name="my_job",
            statuses=[\
                dg.DagsterRunStatus.QUEUED,\
                dg.DagsterRunStatus.NOT_STARTED,\
                dg.DagsterRunStatus.STARTING,\
                dg.DagsterRunStatus.STARTED,\
            ],
        )
    )
    # skip a schedule run if another run of the same job is already running
    if len(run_records) > 0:
        return dg.SkipReason(
            "Skipping this run because another run of the same job is already running"
        )
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[first_asset],
    jobs=[my_job],
    schedules=[my_schedule],
)

```

## Troubleshooting [​](https://docs.dagster.io/guides/operate/managing-concurrency\#troubleshooting "Direct link to Troubleshooting")

When limiting concurrency, you might run into some issues until you get the configuration right.

### Runs going to STARTED status and skipping QUEUED [​](https://docs.dagster.io/guides/operate/managing-concurrency\#runs-going-to-started-status-and-skipping-queued "Direct link to Runs going to STARTED status and skipping QUEUED")

info

This only applies to Dagster Open Source.

If you are running a version older than `1.10.0`, you may need to manually configure your deployment to enable run queueing by setting the `run_queue` key in your instance's settings. In the Dagster UI, navigate to **Deployment > Configuration** and verify that the `run_queue` key is set.

### Runs remaining in QUEUED status [​](https://docs.dagster.io/guides/operate/managing-concurrency\#runs-remaining-in-queued-status "Direct link to Runs remaining in QUEUED status")

The possible causes for runs remaining in `QUEUED` status depend on whether you're using Dagster+ or Dagster Open Source.

- Dagster+
- Dagster Open Source

If runs aren't being dequeued in Dagster+, the root causes could be:

- **If using a [hybrid deployment](https://docs.dagster.io/dagster-plus/deployment/deployment-types/hybrid)**, the agent serving the deployment may be down. In this situation, runs will be paused.
- **Dagster+ is experiencing downtime**. Check the [status page](https://dagstercloud.statuspage.io/) for the latest on potential outages.

If runs aren't being dequeued in Dagster Open Source, the root cause is likely an issue with the Dagster daemon or the run queue configuration.

**Troubleshoot the Dagster daemon**

- **Verify the Dagster daemon is set up and running.** In the Dagster UI, navigate to **Deployment > Daemons** and verify that the daemon is running. The **Run queue** should also be running. If you used [dagster dev](https://docs.dagster.io/guides/operate/webserver) to start the Dagster UI, the daemon should have been started for you. If the daemon isn't running, proceed to step 2.
- **Verify the Dagster daemon can access the same storage as the Dagster webserver process.** Both the webserver process and the Dagster daemon should access the same storage, meaning they should use the same `dagster.yaml`. Locally, this means both processes should have the same set `DAGSTER_HOME` environment variable. If you used dagster dev to start the Dagster UI, both processes should be using the same storage. Refer to the [Dagster Instance docs](https://docs.dagster.io/guides/deploy/dagster-instance-configuration) for more information.

**Troubleshoot the run queue configuration**
If the daemon is running, runs may intentionally be left in the queue due to concurrency rules. To investigate:
\_ **Check the output logged from the daemon process**, as this will include skipped runs.
\_ **Check the max\_concurrent\_runs setting in your instance's dagster.yaml**. If set to 0, this may block the queue. You can check this setting in the Dagster UI by navigating to Deployment > Configuration and locating the concurrency.runs.max\_concurrent\_runs setting. Refer to the [Limit the number of total runs that can be in progress at the same time](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-total-runs-that-can-be-in-progress-at-the-same-time) section for more info. \* **Check the state of your run queue**. In some cases, the queue may be blocked by some number of in-progress runs. To view the status of your run queue, click **Runs** in the top navigation of the Dagster UI and then open the **Queued** and **In Progress** tabs.

If there are queued or in-progress runs blocking the queue, you can terminate them to allow other runs to proceed.

- [Limit the number of total runs that can be in progress at the same time](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-total-runs-that-can-be-in-progress-at-the-same-time)
- [Limit the number of assets or ops actively executing across all runs](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-assets-or-ops-actively-executing-across-all-runs)
- [Limit the number of runs that can be in progress for a set of ops](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-runs-that-can-be-in-progress-for-a-set-of-ops)
  - [Setting a default limit for concurrency pools](https://docs.dagster.io/guides/operate/managing-concurrency#setting-a-default-limit-for-concurrency-pools)
- [Limit the number of runs that can be in progress by run tag](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-runs-that-can-be-in-progress-by-run-tag)
  - [Limit the number of runs that can be in progress by unique tag value](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-runs-that-can-be-in-progress-by-unique-tag-value)
- [Limit the number of ops concurrently executing for a single run](https://docs.dagster.io/guides/operate/managing-concurrency#limit-the-number-of-ops-concurrently-executing-for-a-single-run)
- [Prevent runs from starting if another run is already occurring (advanced)](https://docs.dagster.io/guides/operate/managing-concurrency#prevent-runs-from-starting-if-another-run-is-already-occurring-advanced)
- [Troubleshooting](https://docs.dagster.io/guides/operate/managing-concurrency#troubleshooting)
  - [Runs going to STARTED status and skipping QUEUED](https://docs.dagster.io/guides/operate/managing-concurrency#runs-going-to-started-status-and-skipping-queued)
  - [Runs remaining in QUEUED status](https://docs.dagster.io/guides/operate/managing-concurrency#runs-remaining-in-queued-status)
