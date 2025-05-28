
On this page

## Defining basic schedules [​](https://docs.dagster.io/guides/automate/schedules/defining-schedules\#defining-basic-schedules "Direct link to Defining basic schedules")

The following examples demonstrate how to define some basic schedules.

- Using ScheduleDefinition
- Using @schedule

This example demonstrates how to define a schedule using [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) that will run a job every day at midnight. While this example uses op jobs, the same approach will work with [asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs).

```codeBlockLines_e6Vv
@dg.job
def my_job(): ...

basic_schedule = dg.ScheduleDefinition(job=my_job, cron_schedule="0 0 * * *")

```

note

The `cron_schedule` argument accepts standard [cron expressions](https://en.wikipedia.org/wiki/Cron). If your `croniter` dependency's version is `>= 1.0.12`, the argument will also accept the following:

- `@daily`
- `@hourly`
- `@monthly`

This example demonstrates how to define a schedule using [`@dg.schedule`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.schedule), which provides more flexibility than [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition). For example, you can [configure job behavior based on its scheduled run time](https://docs.dagster.io/guides/automate/schedules/configuring-job-behavior) or [emit log messages](https://docs.dagster.io/guides/automate/schedules/defining-schedules#emitting-log-messages-from-schedule-evaluation).

```codeBlockLines_e6Vv
@schedule(job=my_job, cron_schedule="0 0 * * *")
def basic_schedule(): ...
  # things the schedule does, like returning a RunRequest or SkipReason

```

note

The `cron_schedule` argument accepts standard [cron expressions](https://en.wikipedia.org/wiki/Cron). If your `croniter` dependency's version is `>= 1.0.12`, the argument will also accept the following:

- `@daily`
- `@hourly`
- `@monthly`

## Emitting log messages from schedule evaluation [​](https://docs.dagster.io/guides/automate/schedules/defining-schedules\#emitting-log-messages-from-schedule-evaluation "Direct link to Emitting log messages from schedule evaluation")

This example demonstrates how to emit log messages from a schedule during its evaluation function. These logs will be visible in the UI when you inspect a tick in the schedule's tick history.

```codeBlockLines_e6Vv
@dg.schedule(job=my_job, cron_schedule="* * * * *")
def logs_then_skips(context):
    context.log.info("Logging from a dg.schedule!")
    return dg.SkipReason("Nothing to do")

```

note

Schedule logs are stored in your [Dagster instance's compute log storage](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#compute-log-storage). You should ensure that your compute log storage is configured to view your schedule logs.

- [Defining basic schedules](https://docs.dagster.io/guides/automate/schedules/defining-schedules#defining-basic-schedules)
- [Emitting log messages from schedule evaluation](https://docs.dagster.io/guides/automate/schedules/defining-schedules#emitting-log-messages-from-schedule-evaluation)
