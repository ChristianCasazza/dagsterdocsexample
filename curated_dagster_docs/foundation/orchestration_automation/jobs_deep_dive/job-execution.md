
On this page

note

This guide is applicable to both [ops](https://docs.dagster.io/guides/build/ops/) and [jobs](https://docs.dagster.io/guides/build/jobs/).

Dagster provides several methods to execute [op](https://docs.dagster.io/guides/build/jobs/op-jobs) and [asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs). This guide explains different ways to do one-off execution of jobs using the Dagster UI, command line, or Python APIs.

You can also launch jobs in other ways:

- [Schedules](https://docs.dagster.io/guides/automate/schedules/) can be used to launch runs on a fixed interval.
- [Sensors](https://docs.dagster.io/guides/automate/sensors/) allow you to launch runs based on external state changes.

## Relevant APIs [​](https://docs.dagster.io/guides/build/jobs/job-execution\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`JobDefinition.execute_in_process`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition.execute_in_process) | A method to execute a job synchronously, typically for running scripts or testing. |

## Executing a job [​](https://docs.dagster.io/guides/build/jobs/job-execution\#executing-a-job "Direct link to Executing a job")

Dagster supports the following methods to execute one-off jobs. Click the tabs for more info.

- Dagster UI
- Command line
- Python

Using the Dagster UI, you can view, interact, and execute jobs.

To view your job in the UI, use the [`dagster dev`](https://docs.dagster.io/api/dagster/cli#dagster-dev) command:

```codeBlockLines_e6Vv
dagster dev -f my_job.py

```

Then navigate to `http://localhost:3000`:

![Pipeline def](https://docs.dagster.io/assets/images/pipeline-def-015ce55350bc4f4649a99eadcd33f03c.png)

Click on the **Launchpad** tab, then press the **Launch Run** button to execute the job:

![Job run](https://docs.dagster.io/assets/images/pipeline-run-cbf5e0b0154eae6b4ed1b01f85875098.png)

By default, Dagster will run the job using the [`multiprocess_executor`](https://docs.dagster.io/api/dagster/execution#dagster.multiprocess_executor) \- that means each step in the job runs in its own process, and steps that don't depend on each other can run in parallel.

The Launchpad also offers a configuration editor to let you interactively build up the configuration. Refer to the [run configuration documentation](https://docs.dagster.io/guides/operate/configuration/run-configuration#specifying-runtime-configuration) for more info.

The dagster CLI includes the following commands for job execution:

- [`dagster job execute`](https://docs.dagster.io/api/dagster/cli#dagster-job) for direct execution
- [`dagster job launch`](https://docs.dagster.io/api/dagster/cli#dagster-job) for launching runs asynchronously using the [run launcher](https://docs.dagster.io/guides/deploy/execution/run-launchers) on your instance

To execute your job directly, run:

```codeBlockLines_e6Vv
dagster job execute -f my_job.py

```

### Python APIs [​](https://docs.dagster.io/guides/build/jobs/job-execution\#python-apis "Direct link to Python APIs")

Dagster includes Python APIs for execution that are useful when writing tests or scripts.

[`JobDefinition.execute_in_process`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition.execute_in_process) executes a job and
returns an [`ExecuteInProcessResult`](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult).

```codeBlockLines_e6Vv
Loading...

```

You can find the full API documentation in [Execution API](https://docs.dagster.io/api/dagster/execution) and learn more about the testing use cases in the [testing documentation](https://docs.dagster.io/guides/test/).

## Executing job subsets [​](https://docs.dagster.io/guides/build/jobs/job-execution\#executing-job-subsets "Direct link to Executing job subsets")

Dagster supports ways to run a subset of a job, called **op selection**.

### Op selection syntax [​](https://docs.dagster.io/guides/build/jobs/job-execution\#op-selection-syntax "Direct link to Op selection syntax")

To specify op selection, Dagster supports a simple query syntax.

It works as follows:

- A query includes a list of clauses.
- A clause can be an op name, in which case that op is selected.
- A clause can be an op name preceded by `*`, in which case that op and all of its ancestors (upstream dependencies) are selected.
- A clause can be an op name followed by `*`, in which case that op and all of its descendants (downstream dependencies) are selected.
- A clause can be an op name followed by any number of `+` s, in which case that op and descendants up to that many hops away are selected.
- A clause can be an op name preceded by any number of `+` s, in which case that op and ancestors up to that many hops away are selected.

Let's take a look at some examples:

| Example | Description |
| --- | --- |
| `some_op` | Select `some_op` |
| `*some_op` | Select `some_op` and all ancestors (upstream dependencies). |
| `some_op*` | Select `some_op` and all descendants (downstream dependencies). |
| `*some_op*` | Select `some_op` and all of its ancestors and descendants. |
| `+some_op` | Select `some_op` and its direct parents. |
| `some_op+++` | Select `some_op` and its children, its children's children, and its children's children's children. |

### Specifying op selection [​](https://docs.dagster.io/guides/build/jobs/job-execution\#specifying-op-selection "Direct link to Specifying op selection")

Use this selection syntax in the `op_selection` argument to the [`JobDefinition.execute_in_process`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition.execute_in_process):

```codeBlockLines_e6Vv
    my_job.execute_in_process(op_selection=["*add_two"])

```

Similarly, you can specify the same op selection in the Dagster UI Launchpad:

![Op selection](https://docs.dagster.io/assets/images/solid-selection-6b1244d9173c7ef12cddcaba1084a2d4.png)

## Controlling job execution [​](https://docs.dagster.io/guides/build/jobs/job-execution\#controlling-job-execution "Direct link to Controlling job execution")

Each [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) contains an [`ExecutorDefinition`](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) that determines how it will be executed.

This `executor_def` property can be set to allow for different types of isolation and parallelism, ranging from executing all the ops in the same process to executing each op in its own Kubernetes pod. See [Executors](https://docs.dagster.io/guides/operate/run-executors) for more details.

### Default job executor [​](https://docs.dagster.io/guides/build/jobs/job-execution\#default-job-executor "Direct link to Default job executor")

The default job executor definition defaults to multiprocess execution. It also allows you to toggle between in-process and multiprocess execution via config.

Below is an example of run config as YAML you could provide in the Dagster UI playground to launch an in-process execution.

```codeBlockLines_e6Vv

execution:
  config:
    in_process:

```

Additional config options are available for multiprocess execution that can help with performance. This includes limiting the max concurrent subprocesses and controlling how those subprocesses are spawned.

The example below sets the run config directly on the job to explicitly set the max concurrent subprocesses to `4`, and change the subprocess start method to use a forkserver.

```codeBlockLines_e6Vv
@job(
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "start_method": {
                        "forkserver": {},
                    },
                    "max_concurrent": 4,
                },
            }
        }
    }
)
def forkserver_job():
    multi_three(add_two(return_one()))

```

Using a forkserver is a great way to reduce per-process overhead during multiprocess execution, but can cause issues with certain libraries. Refer to the [Python documentation](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) for more info.

#### Op concurrency limits [​](https://docs.dagster.io/guides/build/jobs/job-execution\#op-concurrency-limits "Direct link to Op concurrency limits")

In addition to the `max_concurrent` limit, you can use `tag_concurrency_limits` to specify limits on the number of ops with certain tags that can execute at once within a single run.

Limits can be specified for all ops with a certain tag key or key-value pair. If any limit would be exceeded by launching an op, then the op will stay queued. Asset jobs will look at the `op_tags` field on each asset in the job when checking them for tag concurrency limits.

For example, the following job will execute at most two ops at once with the `database` tag equal to `redshift`, while also ensuring that at most four ops execute at once:

```codeBlockLines_e6Vv
@job(
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                    "tag_concurrency_limits": [\
                        {\
                            "key": "database",\
                            "value": "redshift",\
                            "limit": 2,\
                        }\
                    ],
                },
            }
        }
    }
)
def tag_concurrency_job(): ...

```

note

These limits are only applied on a per-run basis. You can apply op concurrency limits across multiple runs using the [`celery_executor`](https://docs.dagster.io/api/libraries/dagster-celery#dagster_celery.celery_executor) or [`celery_k8s_job_executor`](https://docs.dagster.io/api/libraries/dagster-celery-k8s#dagster_celery_k8s.celery_k8s_job_executor).

Refer to the [Managing concurrency in data pipelines guide](https://docs.dagster.io/guides/operate/managing-concurrency) for more info about op concurrency, and how to limit run concurrency.

- [Relevant APIs](https://docs.dagster.io/guides/build/jobs/job-execution#relevant-apis)
- [Executing a job](https://docs.dagster.io/guides/build/jobs/job-execution#executing-a-job)
  - [Python APIs](https://docs.dagster.io/guides/build/jobs/job-execution#python-apis)
- [Executing job subsets](https://docs.dagster.io/guides/build/jobs/job-execution#executing-job-subsets)
  - [Op selection syntax](https://docs.dagster.io/guides/build/jobs/job-execution#op-selection-syntax)
  - [Specifying op selection](https://docs.dagster.io/guides/build/jobs/job-execution#specifying-op-selection)
- [Controlling job execution](https://docs.dagster.io/guides/build/jobs/job-execution#controlling-job-execution)
  - [Default job executor](https://docs.dagster.io/guides/build/jobs/job-execution#default-job-executor)
    - [Op concurrency limits](https://docs.dagster.io/guides/build/jobs/job-execution#op-concurrency-limits)
