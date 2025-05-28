
On this page

Dagster offers several ways to run data pipelines without manual intervation, including traditional scheduling and event-based triggers. [Automating your Dagster pipelines](https://docs.dagster.io/guides/automate/) can boost efficiency and ensure that data is produced consistently and reliably.

## Run requests [​](https://docs.dagster.io/api/dagster/schedules-sensors\#run-requests "Direct link to Run requests")

`class` dagster.RunRequest  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_request.py#L66)

Represents all the information required to launch a single run. Must be returned by a
SensorDefinition or ScheduleDefinition’s evaluation function for a run to be launched.

Parameters:

- **run\_key** ( _Optional_ _\[_ _str_ _\]_) – A string key to identify this launched run. For sensors, ensures that only one run is created per run key across all sensor evaluations. For schedules, ensures that one run is created per tick, across failure recoveries. Passing in a None value means that a run will always be launched per evaluation.
- **(** **Optional** **\[** **Union** **\[** **RunConfig** ( _run\_config_) – Configuration for the run. If the job has a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig), this value will override replace the config provided by it.\
- **Mapping** **\[** **str** – Configuration for the run. If the job has a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig), this value will override replace the config provided by it.\
- **Any** **\]** **\]** **\]** – Configuration for the run. If the job has a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig), this value will override replace the config provided by it.
- **tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary of tags (string key-value pairs) to attach to the launched run.
- **job\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the job this run request will launch. Required for sensors that target multiple jobs.
- **asset\_selection** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _\]_) – A subselection of assets that should be launched with this run. If the sensor or schedule targets a job, then by default a RunRequest returned from it will launch all of the assets in the job. If the sensor targets an asset selection, then by default a RunRequest returned from it will launch all the assets in the selection. This argument is used to specify that only a subset of these assets should be launched, instead of all of them.
- **asset\_check\_keys** ( _Optional_ _\[_ _Sequence_ _\[_ [_AssetCheckKey_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetCheckKey) _\]_ _\]_) – A subselection of asset checks that should be launched with this run. This is currently only supported on sensors. If the sensor targets a job, then by default a RunRequest returned from it will launch all of the asset checks in the job. If the sensor targets an asset selection, then by default a RunRequest returned from it will launch all the asset checks in the selection. This argument is used to specify that only a subset of these asset checks should be launched, instead of all of them.
- **stale\_assets\_only** ( _bool_) – Set to true to further narrow the asset selection to stale assets. If passed without an asset selection, all stale assets in the job will be materialized. If the job does not materialize assets, this flag is ignored.
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The partition key for this run request.

`class` dagster.SkipReason  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_request.py#L49)

Represents a skipped evaluation, where no runs are requested. May contain a message to indicate
why no runs were requested.

Parameters: **skip\_message** ( _Optional_ _\[_ _str_ _\]_) – A message displayed in the Dagster UI for why this evaluation resulted
in no requested runs.

## Schedules [​](https://docs.dagster.io/api/dagster/schedules-sensors\#schedules "Direct link to Schedules")

[Schedules](https://docs.dagster.io/guides/automate/schedules/) are Dagster’s way to support traditional ways of automation, such as specifying a job should run at Mondays at 9:00AM. Jobs triggered by schedules can contain a subset of [assets](https://docs.dagster.io/guides/build/assets/) or [ops](https://legacy-docs.dagster.io/concepts/ops-jobs-graphs/ops).

@dagster.schedule  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/schedule_decorator.py#L39)

Creates a schedule following the provided cron schedule and requests runs for the provided job.

The decorated function takes in a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) as its only
argument, and does one of the following:

1. Return a [`RunRequest`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) object.
2. Return a list of [`RunRequest`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) objects.
3. Return a [`SkipReason`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) object, providing a descriptive message of why no runs were requested.
4. Return nothing (skipping without providing a reason)
5. Return a run config dictionary.
6. Yield a [`SkipReason`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) or yield one ore more [`RunRequest`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) objects.
Returns a [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition).

Parameters:

- **cron\_schedule** ( _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A valid cron string or sequence of cron strings specifying when the schedule will run, e.g., `45 23 * * 6` for a schedule that runs at 11:45 PM every Saturday. If a sequence is provided, then the schedule will run for the union of all execution times for the provided cron strings, e.g., `['45 23 * * 6', '30 9 * * 0']` for a schedule that runs at 11:45 PM every Saturday and 9:30 AM every Sunday.
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the schedule.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the schedule and can be used for searching and filtering in the UI.
- **tags\_fn** ( _Optional_ _\[_ _Callable_ _\[_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_ _,_ _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _str_ _\]_ _\]_ _\]_ _\]_) – A function that generates tags to attach to the schedule’s runs. Takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) and returns a dictionary of tags (string key-value pairs). **Note**: Either `tags` or `tags_fn` may be set, but not both.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A set of metadata entries that annotate the schedule. Values will be normalized to typed MetadataValue objects.
- **should\_execute** ( _Optional_ _\[_ _Callable_ _\[_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_ _,_ _bool_ _\]_ _\]_) – A function that runs at schedule execution time to determine whether a schedule should execute or skip. Takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) and returns a boolean ( `True` if the schedule should execute). Defaults to a function that always returns `True`.
- **execution\_timezone** ( _Optional_ _\[_ _str_ _\]_) – Timezone in which the schedule should run. Supported strings for timezones are the ones provided by the [IANA time zone database](https://www.iana.org/time-zones) \- e.g. `"America/Los_Angeles"`.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the schedule.
- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job that should execute when the schedule runs.
- **default\_status** ( _DefaultScheduleStatus_) – If set to `RUNNING`, the schedule will immediately be active when starting Dagster. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – The set of resource keys required by the schedule.
- **target** ( _Optional_ _\[_ _Union_ _\[_ _CoercibleToAssetSelection_ _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The target that the schedule will execute. It can take [`AssetSelection`](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) objects and anything coercible to it (e.g. str, Sequence\[str\], AssetKey, AssetsDefinition). It can also accept [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) (a function decorated with @job is an instance of JobDefinition) and UnresolvedAssetJobDefinition (the return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) objects. This parameter will replace job and job\_name.

`class` dagster.ScheduleDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/schedule_definition.py#L469)

Defines a schedule that targets a job.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the schedule to create. Defaults to the job name plus `_schedule`.

- **cron\_schedule** ( _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A valid cron string or sequence of cron strings specifying when the schedule will run, e.g., `45 23 * * 6` for a schedule that runs at 11:45 PM every Saturday. If a sequence is provided, then the schedule will run for the union of all execution times for the provided cron strings, e.g., `['45 23 * * 6', '30 9 * * 0]` for a schedule that runs at 11:45 PM every Saturday and 9:30 AM every Sunday.

- **execution\_fn** ( _Callable_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_) –

The core evaluation function for the schedule, which is run at an interval to determine whether a run should be launched or not. Takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext).

- **run\_config** ( _Optional_ _\[_ _Union_ _\[_ [_RunConfig_](https://docs.dagster.io/api/dagster/config#dagster.RunConfig) _,_ _Mapping_ _\]_ _\]_) – The config that parameterizes this execution, as a dict.

- **run\_config\_fn** ( _Optional_ _\[_ _Callable_ _\[_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_ _,_ _\[_ _Mapping_ _\]_ _\]_ _\]_) – A function that takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) object and returns the run configuration that parameterizes this execution, as a dict. **Note**: Only one of the following may be set: You may set `run_config`, `run_config_fn`, or `execution_fn`.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the schedule and can be used for searching and filtering in the UI. If no execution\_fn is provided, then these will also be automatically attached to runs launched by the schedule.

- **tags\_fn** ( _Optional_ _\[_ _Callable_ _\[_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_ _,_ _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_ _\]_ _\]_) – A function that generates tags to attach to the schedule’s runs. Takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) and returns a dictionary of tags (string key-value pairs). **Note**: Only one of the following may be set: `tags`, `tags_fn`, or `execution_fn`.

- **should\_execute** ( _Optional_ _\[_ _Callable_ _\[_ _\[_ [_ScheduleEvaluationContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) _\]_ _,_ _bool_ _\]_ _\]_) – A function that runs at schedule execution time to determine whether a schedule should execute or skip. Takes a [`ScheduleEvaluationContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleEvaluationContext) and returns a boolean ( `True` if the schedule should execute). Defaults to a function that always returns `True`.

- **execution\_timezone** ( _Optional_ _\[_ _str_ _\]_) –

- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the schedule.

- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _\]_ _\]_) – The job that should execute when this schedule runs.

- **default\_status** ( _DefaultScheduleStatus_) – If set to `RUNNING`, the schedule will start as running. The default status can be overridden from the Dagster UI or via the GraphQL API.

- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – The set of resource keys required by the schedule.

- **target** ( _Optional_ _\[_ _Union_ _\[_ _CoercibleToAssetSelection_ _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The target that the schedule will execute. It can take [`AssetSelection`](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) objects and anything coercible to it (e.g. str, Sequence\[str\], AssetKey, AssetsDefinition). It can also accept [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) (a function decorated with @job is an instance of JobDefinition) and UnresolvedAssetJobDefinition (the return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) objects. This parameter will replace job and job\_name.

- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A set of metadata entries that annotate the schedule. Values will be normalized to typed MetadataValue objects. Not currently shown in the UI but available at runtime via ScheduleEvaluationContext.repository\_def.get\_schedule\_def(<name>).metadata.


`property` cron\_schedule

The cron schedule representing when this schedule will be evaluated.

Type: Union\[str, Sequence\[str\]\]

`property` default\_status

The default status for this schedule when it is first loaded in
a code location.

Type: DefaultScheduleStatus

`property` description

A description for this schedule.

Type: Optional\[str\]

`property` environment\_vars

deprecated

This API will be removed in version 2.0.
Setting this property no longer has any effect..

Environment variables to export to the cron schedule.

Type: Mapping\[str, str\]

`property` execution\_timezone

The timezone in which this schedule will be evaluated.

Type: Optional\[str\]

`property` job

The job that is
targeted by this schedule.

Type: Union\[ [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition), UnresolvedAssetJobDefinition\]

`property` job\_name

The name of the job targeted by this schedule.

Type: str

`property` metadata

The metadata for this schedule.

Type: Mapping\[str, str\]

`property` name

The name of the schedule.

Type: str

`property` required\_resource\_keys

The set of keys for resources that must be provided to this schedule.

Type: Set\[str\]

`property` tags

The tags for this schedule.

Type: Mapping\[str, str\]

`class` dagster.ScheduleEvaluationContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/schedule_definition.py#L142)

The context object available as the first argument to various functions defined on a [`dagster.ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition).

A `ScheduleEvaluationContext` object is passed as the first argument to `run_config_fn`, `tags_fn`,
and `should_execute`.

**Users should not instantiate this object directly**. To construct a `ScheduleEvaluationContext` for testing purposes, use [`dagster.build_schedule_context()`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.build_schedule_context).

Example:

```codeBlockLines_e6Vv
from dagster import schedule, ScheduleEvaluationContext

@schedule
def the_schedule(context: ScheduleEvaluationContext):
    ...

```

`property` instance

The current [`DagsterInstance`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance).

Type: [DagsterInstance](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)

`property` resources

Mapping of resource key to resource definition to be made available
during schedule execution.

`property` scheduled\_execution\_time

The time in which the execution was scheduled to happen. May differ slightly
from both the actual execution time and the time at which the run config is computed.

dagster.build\_schedule\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/schedule_definition.py#L377)

Builds schedule execution context using the provided parameters.

The instance provided to `build_schedule_context` must be persistent;
[`DagsterInstance.ephemeral()`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) will result in an error.

Parameters:

- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The Dagster instance configured to run the schedule.
- **scheduled\_execution\_time** ( _datetime_) – The time in which the execution was scheduled to happen. May differ slightly from both the actual execution time and the time at which the run config is computed.

Examples:

```codeBlockLines_e6Vv
context = build_schedule_context(instance)

```

dagster.build\_schedule\_from\_partitioned\_job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/partitioned_schedule.py#L63)

Creates a schedule from a job that targets
time window-partitioned or statically-partitioned assets. The job can also be
multi-partitioned, as long as one of the partition dimensions is time-partitioned.

The schedule executes at the cadence specified by the time partitioning of the job or assets.

**Example:**

```codeBlockLines_e6Vv
######################################
# Job that targets partitioned assets
######################################

from dagster import (
    DailyPartitionsDefinition,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
    Definitions,
)

@asset(partitions_def=DailyPartitionsDefinition(start_date="2020-01-01"))
def asset1():
    ...

asset1_job = define_asset_job("asset1_job", selection=[asset1])

# The created schedule will fire daily
asset1_job_schedule = build_schedule_from_partitioned_job(asset1_job)

defs = Definitions(assets=[asset1], schedules=[asset1_job_schedule])

################
# Non-asset job
################

from dagster import DailyPartitionsDefinition, build_schedule_from_partitioned_job, jog

@job(partitions_def=DailyPartitionsDefinition(start_date="2020-01-01"))
def do_stuff_partitioned():
    ...

# The created schedule will fire daily
do_stuff_partitioned_schedule = build_schedule_from_partitioned_job(
    do_stuff_partitioned,
)

defs = Definitions(schedules=[do_stuff_partitioned_schedule])

```

dagster.\_core.scheduler.DagsterDaemonScheduler Scheduler  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/scheduler/scheduler.py#L214)

Default scheduler implementation that submits runs from the long-lived `dagster-daemon`
process. Periodically checks each running schedule for execution times that don’t yet
have runs and launches them.

## Sensors [​](https://docs.dagster.io/api/dagster/schedules-sensors\#sensors "Direct link to Sensors")

[Sensors](https://docs.dagster.io/guides/automate/sensors/) are typically used to poll, listen, and respond to external events. For example, you could configure a sensor to run a job or materialize an asset in response to specific events.

@dagster.sensor  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/sensor_decorator.py#L35)

Creates a sensor where the decorated function is used as the sensor’s evaluation function.

The decorated function may:

1. Return a RunRequest object.
2. Return a list of RunRequest objects.
3. Return a SkipReason object, providing a descriptive message of why no runs were requested.
4. Return nothing (skipping without providing a reason)
5. Yield a SkipReason or yield one or more RunRequest objects.
Takes a `SensorEvaluationContext`.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the sensor. Defaults to the name of the decorated function.
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job to be executed when the sensor fires.
- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed when the sensor fires.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **asset\_selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) – An asset selection to launch a run for if the sensor condition is met. This can be provided instead of specifying a job.
- **required\_resource\_keys** ( _Optional_ _\[_ _set_ _\[_ _str_ _\]_ _\]_) – A set of resource keys that must be available on the context when the sensor evaluation function runs. Use this to specify resources your sensor function depends on.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.
- **target** ( _Optional_ _\[_ _Union_ _\[_ _CoercibleToAssetSelection_ _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The target that the sensor will execute. It can take [`AssetSelection`](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) objects and anything coercible to it (e.g. str, Sequence\[str\], AssetKey, AssetsDefinition). It can also accept [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) (a function decorated with @job is an instance of JobDefinition) and UnresolvedAssetJobDefinition (the return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) objects. This is a parameter that will replace job, jobs, and asset\_selection.

`class` dagster.SensorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/sensor_definition.py#L556)

Define a sensor that initiates a set of runs based on some external state.

Parameters:

- **evaluation\_fn** ( _Callable_ _\[_ _\[_ _SensorEvaluationContext_ _\]_ _\]_) –

The core evaluation function for the sensor, which is run at an interval to determine whether a run should be launched or not. Takes a `SensorEvaluationContext`.

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the sensor to create. Defaults to name of evaluation\_fn

- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.

- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.

- **job** ( _Optional_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJob_ _\]_) – The job to execute when this sensor fires.

- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJob_ _\]_ _\]_) – A list of jobs to execute when this sensor fires.

- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.

- **asset\_selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) – An asset selection to launch a run for if the sensor condition is met. This can be provided instead of specifying a job.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.

- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects. Not currently shown in the UI but available at runtime via SensorEvaluationContext.repository\_def.get\_sensor\_def(<name>).metadata.

- **target** ( _Optional_ _\[_ _Union_ _\[_ _CoercibleToAssetSelection_ _,_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The target that the sensor will execute. It can take [`AssetSelection`](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) objects and anything coercible to it (e.g. str, Sequence\[str\], AssetKey, AssetsDefinition). It can also accept [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) (a function decorated with @job is an instance of JobDefinition) and UnresolvedAssetJobDefinition (the return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) objects. This is a parameter that will replace job, jobs, and asset\_selection.


`property` default\_status

The default status for this sensor when it is first loaded in
a code location.

Type: DefaultSensorStatus

`property` description

A description for this sensor.

Type: Optional\[str\]

`property` job

The job that is
targeted by this schedule.

Type: Union\[ [GraphDefinition](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition), [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition), UnresolvedAssetJobDefinition\]

`property` job\_name

The name of the job that is targeted by this sensor.

Type: Optional\[str\]

`property` jobs

A list of jobs
that are targeted by this schedule.

Type: List\[Union\[ [GraphDefinition](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition), [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition), UnresolvedAssetJobDefinition\]\]

`property` minimum\_interval\_seconds

The minimum number of seconds between sequential evaluations of this sensor.

Type: Optional\[int\]

`property` name

The name of this sensor.

Type: str

`property` required\_resource\_keys

The set of keys for resources that must be provided to this sensor.

Type: Set\[str\]

`class` dagster.SensorEvaluationContext [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/sensor_definition.py#L103)

The context object available as the argument to the evaluation function of a [`dagster.SensorDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SensorDefinition).

Users should not instantiate this object directly. To construct a
SensorEvaluationContext for testing purposes, use `dagster.     build_sensor_context()`.

Parameters:

- **instance\_ref** ( _Optional_ _\[_ [_InstanceRef_](https://docs.dagster.io/api/dagster/internals#dagster._core.instance.InstanceRef) _\]_) – The serialized instance configured to run the schedule
- **cursor** ( _Optional_ _\[_ _str_ _\]_) – The cursor, passed back from the last sensor evaluation via the cursor attribute of SkipReason and RunRequest
- **last\_tick\_completion\_time** ( _float_) – The last time that the sensor was evaluated (UTC).
- **last\_run\_key** ( _str_) – DEPRECATED The run key of the RunRequest most recently created by this sensor. Use the preferred cursor attribute instead.
- **log\_key** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – The log key to use for this sensor tick.
- **repository\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the repository that the sensor belongs to.
- **repository\_def** ( _Optional_ _\[_ [_RepositoryDefinition_](https://docs.dagster.io/api/dagster/repositories#dagster.RepositoryDefinition) _\]_) – The repository or that the sensor belongs to. If needed by the sensor top-level resource definitions will be pulled from this repository. You can provide either this or definitions.
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The deserialized instance can also be passed in directly (primarily useful in testing contexts).
- **definitions** ( _Optional_ _\[_ [_Definitions_](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) _\]_) – Definitions object that the sensor is defined in. If needed by the sensor, top-level resource definitions will be pulled from these definitions. You can provide either this or repository\_def.
- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of resource keys to resource definitions to be made available during sensor execution.
- **last\_sensor\_start\_time** ( _float_) – The last time that the sensor was started (UTC).
- **code\_location\_origin** ( _Optional_ _\[_ _CodeLocationOrigin_ _\]_) – The code location that the sensor is in.

Example:

```codeBlockLines_e6Vv
from dagster import sensor, SensorEvaluationContext

@sensor
def the_sensor(context: SensorEvaluationContext):
    ...

```

update\_cursor [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/sensor_definition.py#L381)

Updates the cursor value for this sensor, which will be provided on the context for the
next sensor evaluation.

This can be used to keep track of progress and avoid duplicate work across sensor
evaluations.

Parameters: **cursor** ( _Optional_ _\[_ _str_ _\]_)

`property` cursor

The cursor value for this sensor, which was set in an earlier sensor evaluation.

`property` instance

The current DagsterInstance.

Type: [DagsterInstance](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)

`property` is\_first\_tick\_since\_sensor\_start

Flag representing if this is the first tick since the sensor was started.

`property` last\_run\_key

The run key supplied to the most recent RunRequest produced by this sensor.

Type: Optional\[str\]

`property` last\_sensor\_start\_time

Timestamp representing the last time this sensor was started. Can be
used in concert with last\_tick\_completion\_time to determine if this is the first tick since the
sensor was started.

Type: Optional\[float\]

`property` last\_tick\_completion\_time

Timestamp representing the last time this sensor completed an evaluation.

Type: Optional\[float\]

`property` repository\_def

The RepositoryDefinition that this sensor resides in.

Type: Optional\[ [RepositoryDefinition](https://docs.dagster.io/api/dagster/repositories#dagster.RepositoryDefinition)\]

`property` repository\_name

The name of the repository that this sensor resides in.

Type: Optional\[str\]

`property` resources

A mapping from resource key to instantiated resources for this sensor.

Type: Resources

dagster.build\_sensor\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/sensor_definition.py#L1260)

Builds sensor execution context using the provided parameters.

This function can be used to provide a context to the invocation of a sensor definition.If
provided, the dagster instance must be persistent; DagsterInstance.ephemeral() will result in an
error.

Parameters:

- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured to run the sensor.
- **cursor** ( _Optional_ _\[_ _str_ _\]_) – A cursor value to provide to the evaluation of the sensor.
- **repository\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the repository that the sensor belongs to.
- **repository\_def** ( _Optional_ _\[_ [_RepositoryDefinition_](https://docs.dagster.io/api/dagster/repositories#dagster.RepositoryDefinition) _\]_) – The repository that the sensor belongs to. If needed by the sensor top-level resource definitions will be pulled from this repository. You can provide either this or definitions.
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_ResourceDefinition_](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) _\]_ _\]_) – A set of resource definitions to provide to the sensor. If passed, these will override any resource definitions provided by the repository.
- **definitions** ( _Optional_ _\[_ [_Definitions_](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) _\]_) – Definitions object that the sensor is defined in. If needed by the sensor, top-level resource definitions will be pulled from these definitions. You can provide either this or repository\_def.
- **last\_sensor\_start\_time** ( _Optional_ _\[_ _float_ _\]_) – The last time the sensor was started.

Examples:

```codeBlockLines_e6Vv
context = build_sensor_context()
my_sensor(context)

```

@dagster.asset\_sensor  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/sensor_decorator.py#L123)

Creates an asset sensor where the decorated function is used as the asset sensor’s evaluation
function.

If the asset has been materialized multiple times between since the last sensor tick, the
evaluation function will only be invoked once, with the latest materialization.

The decorated function may:

1. Return a RunRequest object.
2. Return a list of RunRequest objects.
3. Return a SkipReason object, providing a descriptive message of why no runs were requested.
4. Return nothing (skipping without providing a reason)
5. Yield a SkipReason or yield one or more RunRequest objects.
Takes a `SensorEvaluationContext` and an EventLogEntry corresponding to an
AssetMaterialization event.

Parameters:

- **asset\_key** ( [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)) – The asset\_key this sensor monitors.
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the sensor. Defaults to the name of the decorated function.
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job to be executed when the sensor fires.
- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed when the sensor fires.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI. Values that are not already strings will be serialized as JSON.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.

Example:

```codeBlockLines_e6Vv
from dagster import AssetKey, EventLogEntry, SensorEvaluationContext, asset_sensor

@asset_sensor(asset_key=AssetKey("my_table"), job=my_job)
def my_asset_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    return RunRequest(
        run_key=context.cursor,
        run_config={
            "ops": {
                "read_materialization": {
                    "config": {
                        "asset_key": asset_event.dagster_event.asset_key.path,
                    }
                }
            }
        },
    )

```

@dagster.multi\_asset\_sensor  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/sensor_decorator.py#L251)

Creates an asset sensor that can monitor multiple assets.

The decorated function is used as the asset sensor’s evaluation
function. The decorated function may:

1. Return a RunRequest object.
2. Return a list of RunRequest objects.
3. Return a SkipReason object, providing a descriptive message of why no runs were requested.
4. Return nothing (skipping without providing a reason)
5. Yield a SkipReason or yield one or more RunRequest objects.
Takes a `MultiAssetSensorEvaluationContext`.

Parameters:

- **monitored\_assets** ( _Union_ _\[_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_) – The assets this sensor monitors. If an AssetSelection object is provided, it will only apply to assets within the Definitions that this sensor is part of.
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the sensor. Defaults to the name of the decorated function.
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job to be executed when the sensor fires.
- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed when the sensor fires.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **request\_assets** ( _Optional_ _\[_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_) – An asset selection to launch a run for if the sensor condition is met. This can be provided instead of specifying a job.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.

@dagster.run\_status\_sensor  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L1023)

Creates a sensor that reacts to a given status of job execution, where the decorated
function will be run when a job is at the given status.

Takes a [`RunStatusSensorContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunStatusSensorContext).

Parameters:

- **run\_status** ( [_DagsterRunStatus_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRunStatus)) – The status of run execution which will be monitored by the sensor.
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the sensor. Defaults to the name of the decorated function.
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **monitored\_jobs** ( _Optional_ _\[_ _List_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ _UnresolvedAssetJobDefinition_ _,_ [_RepositorySelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RepositorySelector) _,_ [_JobSelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.JobSelector) _,_ _CodeLocationSelector_ _\]_ _\]_ _\]_) – Jobs in the current code locations that will be monitored by this sensor. Defaults to None, which means the alert will be sent when any job in the code location matches the requested run\_status. Jobs in external repositories can be monitored by using RepositorySelector or JobSelector.
- **monitor\_all\_code\_locations** ( _Optional_ _\[_ _bool_ _\]_) – If set to True, the sensor will monitor all runs in the Dagster deployment. If set to True, an error will be raised if you also specify monitored\_jobs or job\_selection. Defaults to False.
- **job\_selection** ( _Optional_ _\[_ _List_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_RepositorySelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RepositorySelector) _,_ [_JobSelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.JobSelector) _,_ _CodeLocationSelector_ _\]_ _\]_ _\]_) – deprecated (deprecated in favor of monitored\_jobs) Jobs in the current code location that will be monitored by this sensor. Defaults to None, which means the alert will be sent when any job in the code location matches the requested run\_status.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **request\_job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job that should be executed if a RunRequest is yielded from the sensor.
- **request\_jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed if RunRequests are yielded from the sensor.
- **monitor\_all\_repositories** ( _Optional_ _\[_ _bool_ _\]_) – deprecated (deprecated in favor of monitor\_all\_code\_locations) If set to True, the sensor will monitor all runs in the Dagster instance. If set to True, an error will be raised if you also specify monitored\_jobs or job\_selection. Defaults to False.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.

@dagster.run\_failure\_sensor  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L441)

Creates a sensor that reacts to job failure events, where the decorated function will be
run when a run fails.

Takes a [`RunFailureSensorContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunFailureSensorContext).

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the job failure sensor. Defaults to the name of the decorated function.
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **monitored\_jobs** ( _Optional_ _\[_ _List_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ _UnresolvedAssetJobDefinition_ _,_ [_RepositorySelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RepositorySelector) _,_ [_JobSelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.JobSelector) _,_ _CodeLocationSelector_ _\]_ _\]_ _\]_) – The jobs in the current repository that will be monitored by this failure sensor. Defaults to None, which means the alert will be sent when any job in the current repository fails.
- **monitor\_all\_code\_locations** ( _bool_) – If set to True, the sensor will monitor all runs in the Dagster deployment. If set to True, an error will be raised if you also specify monitored\_jobs or job\_selection. Defaults to False.
- **job\_selection** ( _Optional_ _\[_ _List_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_RepositorySelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RepositorySelector) _,_ [_JobSelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.JobSelector) _,_ _CodeLocationSelector_ _\]_ _\]_ _\]_) – deprecated (deprecated in favor of monitored\_jobs) The jobs in the current repository that will be monitored by this failure sensor. Defaults to None, which means the alert will be sent when any job in the repository fails.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **request\_job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJob_ _\]_ _\]_) – The job a RunRequest should execute if yielded from the sensor.
- **request\_jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJob_ _\]_ _\]_ _\]_) – A list of jobs to be executed if RunRequests are yielded from the sensor.
- **monitor\_all\_repositories** ( _bool_) – deprecated (deprecated in favor of monitor\_all\_code\_locations) If set to True, the sensor will monitor all runs in the Dagster instance. If set to True, an error will be raised if you also specify monitored\_jobs or job\_selection. Defaults to False.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.

`class` dagster.AssetSensorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/asset_sensor_definition.py#L45)

Define an asset sensor that initiates a set of runs based on the materialization of a given
asset.

If the asset has been materialized multiple times between since the last sensor tick, the
evaluation function will only be invoked once, with the latest materialization.

Parameters:

- **name** ( _str_) – The name of the sensor to create.

- **asset\_key** ( [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)) – The asset\_key this sensor monitors.

- **asset\_materialization\_fn** ( _Callable_ _\[_ _\[_ _SensorEvaluationContext_ _,_ [_EventLogEntry_](https://docs.dagster.io/api/dagster/internals#dagster.EventLogEntry) _\]_ _,_ _Union_ _\[_ _Iterator_ _\[_ _Union_ _\[_ [_RunRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) _,_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _\]_ _\]_ _,_ [_RunRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) _,_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _\]_ _\]_) –

The core evaluation function for the sensor, which is run at an interval to determine whether a run should be launched or not. Takes a `SensorEvaluationContext` and an EventLogEntry corresponding to an AssetMaterialization event.

- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.

- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.

- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job object to target with this sensor.

- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed when the sensor fires.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.

- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.

- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.


`property` asset\_key

The key of the asset targeted by this sensor.

Type: [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)

`class` dagster.MultiAssetSensorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/multi_asset_sensor_definition.py#L1098)

superseded

This API has been superseded.
For most use cases, Declarative Automation should be used instead of multi\_asset\_sensors to monitor the status of upstream assets and launch runs in response. In cases where side effects are required, or a specific job must be targeted for execution, multi\_asset\_sensors may be used..

Define an asset sensor that initiates a set of runs based on the materialization of a list of
assets.

Users should not instantiate this object directly. To construct a
MultiAssetSensorDefinition, use `dagster.     multi_asset_sensor()`.

Parameters:

- **name** ( _str_) – The name of the sensor to create.

- **asset\_keys** ( _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_) – The asset\_keys this sensor monitors.

- **asset\_materialization\_fn** ( _Callable_ _\[_ _\[_ _MultiAssetSensorEvaluationContext_ _\]_ _,_ _Union_ _\[_ _Iterator_ _\[_ _Union_ _\[_ [_RunRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) _,_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _\]_ _\]_ _,_ [_RunRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) _,_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _\]_ _\]_) –

The core evaluation function for the sensor, which is run at an interval to determine whether a run should be launched or not. Takes a `MultiAssetSensorEvaluationContext`.

- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.

- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.

- **job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_) – The job object to target with this sensor.

- **jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – A list of jobs to be executed when the sensor fires.

- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.

- **request\_assets** ( _Optional_ _\[_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_) – an asset selection to launch a run for if the sensor condition is met. This can be provided instead of specifying a job.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.

- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.


`class` dagster.RunStatusSensorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L581)

Define a sensor that reacts to a given status of job execution, where the decorated
function will be evaluated when a run is at the given status.

Parameters:

- **name** ( _str_) – The name of the sensor. Defaults to the name of the decorated function.
- **run\_status** ( [_DagsterRunStatus_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRunStatus)) – The status of a run which will be monitored by the sensor.
- **run\_status\_sensor\_fn** ( _Callable_ _\[_ _\[_ [_RunStatusSensorContext_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunStatusSensorContext) _\]_ _,_ _Union_ _\[_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _,_ _DagsterRunReaction_ _\]_ _\]_) – The core evaluation function for the sensor. Takes a [`RunStatusSensorContext`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunStatusSensorContext).
- **minimum\_interval\_seconds** ( _Optional_ _\[_ _int_ _\]_) – The minimum number of seconds that will elapse between sensor evaluations.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the sensor.
- **monitored\_jobs** ( _Optional_ _\[_ _List_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ _UnresolvedAssetJobDefinition_ _,_ [_JobSelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.JobSelector) _,_ [_RepositorySelector_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RepositorySelector) _,_ _CodeLocationSelector_ _\]_ _\]_ _\]_) – The jobs in the current repository that will be monitored by this sensor. Defaults to None, which means the alert will be sent when any job in the repository fails.
- **monitor\_all\_code\_locations** ( _bool_) – If set to True, the sensor will monitor all runs in the Dagster deployment. If set to True, an error will be raised if you also specify monitored\_jobs or job\_selection. Defaults to False.
- **default\_status** ( _DefaultSensorStatus_) – Whether the sensor starts as running or not. The default status can be overridden from the Dagster UI or via the GraphQL API.
- **request\_job** ( _Optional_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _\]_ _\]_) – The job a RunRequest should execute if yielded from the sensor.
- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A set of key-value tags that annotate the sensor and can be used for searching and filtering in the UI.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of metadata entries that annotate the sensor. Values will be normalized to typed MetadataValue objects.
- **request\_jobs** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_GraphDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) _,_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _\]_ _\]_ _\]_) – A list of jobs to be executed if RunRequests are yielded from the sensor.

`class` dagster.RunStatusSensorContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L126)

The `context` object available to a decorated function of `run_status_sensor`.

`property` dagster\_event

The event associated with the job run status.

`property` dagster\_run

The run of the job.

`property` instance

The current instance.

`property` log

The logger for the current sensor evaluation.

`property` partition\_key

The partition key of the relevant run.

Type: Optional\[str\]

`property` sensor\_name

The name of the sensor.

`class` dagster.RunFailureSensorContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L298)

The `context` object available to a decorated function of `run_failure_sensor`.

Parameters:

- **sensor\_name** ( _str_) – the name of the sensor.
- **dagster\_run** ( [_DagsterRun_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun)) – the failed run.

get\_step\_failure\_events  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L316)

The step failure event for each step in the run that failed.

Examples:

```codeBlockLines_e6Vv
error_strings_by_step_key = {
    # includes the stack trace
    event.step_key: event.event_specific_data.error.to_string()
    for event in context.get_step_failure_events()
}

```

`property` failure\_event

The run failure event.

If the run failed because of an error inside a step, get\_step\_failure\_events will have more
details on the step failure.

`class` dagster.JobSelector  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/selector.py#L77)`class` dagster.RepositorySelector  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/selector.py#L132)dagster.build\_run\_status\_sensor\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_status_sensor_definition.py#L335)

Builds run status sensor context from provided parameters.

This function can be used to provide the context argument when directly invoking a function
decorated with @run\_status\_sensor or @run\_failure\_sensor, such as when writing unit tests.

Parameters:

- **sensor\_name** ( _str_) – The name of the sensor the context is being constructed for.
- **dagster\_event** ( [_DagsterEvent_](https://docs.dagster.io/api/dagster/execution#dagster.DagsterEvent)) – A DagsterEvent with the same event type as the one that triggers the run\_status\_sensor
- **dagster\_instance** ( [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)) – The dagster instance configured for the context.
- **dagster\_run** ( [_DagsterRun_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun)) – DagsterRun object from running a job
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A dictionary of resources to be made available to the sensor.
- **repository\_def** ( _Optional_ _\[_ [_RepositoryDefinition_](https://docs.dagster.io/api/dagster/repositories#dagster.RepositoryDefinition) _\]_) – beta The repository that the sensor belongs to.

Examples:

```codeBlockLines_e6Vv
instance = DagsterInstance.ephemeral()
result = my_job.execute_in_process(instance=instance)

dagster_run = result.dagster_run
dagster_event = result.get_job_success_event() # or get_job_failure_event()

context = build_run_status_sensor_context(
    sensor_name="run_status_sensor_to_invoke",
    dagster_instance=instance,
    dagster_run=dagster_run,
    dagster_event=dagster_event,
)
run_status_sensor_to_invoke(context)

```

`class` dagster.SensorResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/run_request.py#L358)

The result of a sensor evaluation.

Parameters:

- **run\_requests** ( _Optional_ _\[_ _Sequence_ _\[_ [_RunRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest) _\]_ _\]_) – A list of run requests to be executed.
- **skip\_reason** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ [_SkipReason_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SkipReason) _\]_ _\]_) – A skip message indicating why sensor evaluation was skipped.
- **cursor** ( _Optional_ _\[_ _str_ _\]_) – The cursor value for this sensor, which will be provided on the context for the next sensor evaluation.
- **dynamic\_partitions\_requests** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_DeleteDynamicPartitionsRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.DeleteDynamicPartitionsRequest) _,_ [_AddDynamicPartitionsRequest_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.AddDynamicPartitionsRequest) _\]_ _\]_ _\]_) – A list of dynamic partition requests to request dynamic partition addition and deletion. Run requests will be evaluated using the state of the partitions with these changes applied. We recommend limiting partition additions and deletions to a maximum of 25K partitions per sensor evaluation, as this is the maximum recommended partition limit per asset.
- **asset\_events** ( _Optional_ _\[_ _Sequence_ _\[_ _Union_ _\[_ [_AssetObservation_](https://docs.dagster.io/api/dagster/assets#dagster.AssetObservation) _,_ [_AssetMaterialization_](https://docs.dagster.io/api/dagster/ops#dagster.AssetMaterialization) _,_ _AssetCheckEvaluation_ _\]_ _\]_ _\]_) – A list of materializations, observations, and asset check evaluations that the system will persist on your behalf at the end of sensor evaluation. These events will be not be associated with any particular run, but will be queryable and viewable in the asset catalog.

`class` dagster.AddDynamicPartitionsRequest  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/dynamic_partitions_request.py#L9)

A request to add partitions to a dynamic partitions definition, to be evaluated by a sensor or schedule.

`class` dagster.DeleteDynamicPartitionsRequest  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/dynamic_partitions_request.py#L33)

A request to delete partitions to a dynamic partitions definition, to be evaluated by a sensor or schedule.

- [Run requests](https://docs.dagster.io/api/dagster/schedules-sensors#run-requests)
- [Schedules](https://docs.dagster.io/api/dagster/schedules-sensors#schedules)
- [Sensors](https://docs.dagster.io/api/dagster/schedules-sensors#sensors)
