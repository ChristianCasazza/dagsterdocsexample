
On this page

note

Looking to materialize [asset definitions](https://docs.dagster.io/guides/build/assets/) instead of ops? Check out the [asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs) documentation.

[Jobs](https://docs.dagster.io/guides/build/jobs/) are the main unit of execution and monitoring in Dagster. An op job executes a [graph](https://docs.dagster.io/guides/build/ops/graphs) of [ops](https://docs.dagster.io/guides/build/ops/).

Op jobs can be launched in a few different ways:

- Manually from the Dagster UI
- At fixed intervals, by [schedules](https://docs.dagster.io/guides/automate/schedules/)
- When external changes occur, using [sensors](https://docs.dagster.io/guides/automate/sensors/)

## Relevant APIs [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`@dg.job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) | The decorator used to define a job. |
| [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) | A job definition. Jobs are the main unit of execution and monitoring in Dagster. Typically constructed using the [`@dg.job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) decorator. |

## Creating op jobs [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#creating-op-jobs "Direct link to Creating op jobs")

Op jobs can be created:

- [Using the `@job` decorator](https://docs.dagster.io/guides/build/jobs/op-jobs#using-the-job-decorator)
- [From a graph](https://docs.dagster.io/guides/build/jobs/op-jobs#from-a-graph)

### Using the @job decorator [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#using-the-job-decorator "Direct link to Using the @job decorator")

The simplest way to create an op job is to use the [`@dg.job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) decorator.

Within the decorated function body, you can use function calls to indicate the dependency structure between the ops/graphs. This allows you to explicitly define dependencies between ops when you define the job.

In this example, the `add_one` op depends on the `return_five` op's output. Because this data dependency exists, the `add_one` op executes after `return_five` runs successfully and emits the required output:

```codeBlockLines_e6Vv
from dagster import job, op

@op
def return_five():
    return 5

@op
def add_one(arg):
    return arg + 1

@job
def do_stuff():
    add_one(return_five())

```

When defining an op job, you can provide any of the following:

- [Resources](https://docs.dagster.io/guides/build/external-resources/)
- [Configuration](https://docs.dagster.io/guides/operate/configuration/)
- [Hooks](https://docs.dagster.io/guides/build/ops/op-hooks)
- [Tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags/tags) and other metadata
- An [executor](https://docs.dagster.io/guides/operate/run-executors)

### From a graph [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#from-a-graph "Direct link to From a graph")

Creating op jobs from a graph can be useful when you want to define inter-op dependencies before binding them to resources, configuration, executors, and other environment-specific features. This approach to op job creation allows you to customize graphs for each environment by plugging in configuration and services specific to that environment.

You can model this by building multiple op jobs that use the same underlying graph of ops. The graph represents the logical core of data transformation, and the configuration and resources on each op job customize the behavior of that job for its environment.

To do this, define a graph using the [`@dg.graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph) decorator:

```codeBlockLines_e6Vv
from dagster import graph, op, ConfigurableResource

class Server(ConfigurableResource):
    def ping_server(self): ...

@op
def interact_with_server(server: Server):
    server.ping_server()

@graph
def do_stuff():
    interact_with_server()

```

Then build op jobs from it using the [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) method:

```codeBlockLines_e6Vv
from dagster import ResourceDefinition

prod_server = ResourceDefinition.mock_resource()
local_server = ResourceDefinition.mock_resource()

prod_job = do_stuff.to_job(resource_defs={"server": prod_server}, name="do_stuff_prod")
local_job = do_stuff.to_job(
    resource_defs={"server": local_server}, name="do_stuff_local"
)

```

`to_job` accepts the same arguments as the [`@dg.job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) decorator, such as providing resources, configuration, etc.

## Configuring op jobs [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#configuring-op-jobs "Direct link to Configuring op jobs")

Ops and resources can accept [configuration](https://docs.dagster.io/guides/operate/configuration/run-configuration) that determines how they behave. By default, configuration is supplied at the time an op job is launched.

When constructing an op job, you can customize how that configuration will be satisfied by passing a value to the `config` parameter of the [`GraphDefinition.to_job`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition.to_job) method or the [`@dg.job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) decorator.

The options are discussed below:

- [Hardcoded configuration](https://docs.dagster.io/guides/build/jobs/op-jobs#hardcoded-configuration)
- [Partitioned configuration](https://docs.dagster.io/guides/build/jobs/op-jobs#partitioned-configuration)
- [Config mapping](https://docs.dagster.io/guides/build/jobs/op-jobs#config-mapping)

### Hardcoded configuration [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#hardcoded-configuration "Direct link to Hardcoded configuration")

You can supply a [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig) object or raw config dictionary. The supplied config will be used to configure the op job whenever it's launched. It will show up in the Dagster UI Launchpad and can be overridden.

```codeBlockLines_e6Vv
from dagster import Config, OpExecutionContext, RunConfig, job, op

class DoSomethingConfig(Config):
    config_param: str

@op
def do_something(context: OpExecutionContext, config: DoSomethingConfig):
    context.log.info("config_param: " + config.config_param)

default_config = RunConfig(
    ops={"do_something": DoSomethingConfig(config_param="stuff")}
)

@job(config=default_config)
def do_it_all_with_default_config():
    do_something()

```

### Partitioned configuration [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#partitioned-configuration "Direct link to Partitioned configuration")

Supplying a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig) will create a partitioned op job. This defines a discrete set of partitions along with a function for generating config for a partition. Op job runs can be configured by selecting a partition.

Refer to the [Partitioning ops documentation](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops) for more info and examples.

### Config mapping [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#config-mapping "Direct link to Config mapping")

Supplying a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) allows you to expose a narrower config interface to the op job.

Instead of needing to configure every op and resource individually when launching the op job, you can supply a smaller number of values to the outer config. The [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) will then translate it into config for all the job's ops and resources.

```codeBlockLines_e6Vv
from dagster import Config, OpExecutionContext, RunConfig, config_mapping, job, op

class DoSomethingConfig(Config):
    config_param: str

@op
def do_something(context: OpExecutionContext, config: DoSomethingConfig) -> None:
    context.log.info("config_param: " + config.config_param)

class SimplifiedConfig(Config):
    simplified_param: str

@config_mapping
def simplified_config(val: SimplifiedConfig) -> RunConfig:
    return RunConfig(
        ops={"do_something": DoSomethingConfig(config_param=val.simplified_param)}
    )

@job(config=simplified_config)
def do_it_all_with_simplified_config():
    do_something()

```

## Making op jobs available to Dagster tools [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#making-op-jobs-available-to-dagster-tools "Direct link to Making op jobs available to Dagster tools")

You make jobs available to the UI, GraphQL, and command line by including them in a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object at the top level of Python module or file. The tool loads that module as a [code location](https://docs.dagster.io/guides/deploy/code-locations/). If you include schedules or sensors, the code location will automatically include jobs that those schedules or sensors target.

```codeBlockLines_e6Vv
from dagster import Definitions, job

@job
def do_it_all(): ...

defs = Definitions(jobs=[do_it_all])

```

## Testing op jobs [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#testing-op-jobs "Direct link to Testing op jobs")

Dagster has built-in support for testing, including separating business logic from environments and setting explicit expectations on uncontrollable inputs. Refer to the [testing documentation](https://docs.dagster.io/guides/test/) for more info and examples.

## Executing op jobs [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#executing-op-jobs "Direct link to Executing op jobs")

You can run an op job in a variety of ways:

- In the Python process where it's defined
- Via the command line
- Via the GraphQL API
- In [the UI](https://docs.dagster.io/guides/operate/webserver#dagster-ui-reference). The UI centers on jobs, making it a one-stop shop - you can manually kick off runs for an op job and view all historical runs.

Refer to the [Job execution guide](https://docs.dagster.io/guides/build/jobs/job-execution) for more info and examples.

- [Relevant APIs](https://docs.dagster.io/guides/build/jobs/op-jobs#relevant-apis)
- [Creating op jobs](https://docs.dagster.io/guides/build/jobs/op-jobs#creating-op-jobs)
  - [Using the @job decorator](https://docs.dagster.io/guides/build/jobs/op-jobs#using-the-job-decorator)
  - [From a graph](https://docs.dagster.io/guides/build/jobs/op-jobs#from-a-graph)
- [Configuring op jobs](https://docs.dagster.io/guides/build/jobs/op-jobs#configuring-op-jobs)
  - [Hardcoded configuration](https://docs.dagster.io/guides/build/jobs/op-jobs#hardcoded-configuration)
  - [Partitioned configuration](https://docs.dagster.io/guides/build/jobs/op-jobs#partitioned-configuration)
  - [Config mapping](https://docs.dagster.io/guides/build/jobs/op-jobs#config-mapping)
- [Making op jobs available to Dagster tools](https://docs.dagster.io/guides/build/jobs/op-jobs#making-op-jobs-available-to-dagster-tools)
- [Testing op jobs](https://docs.dagster.io/guides/build/jobs/op-jobs#testing-op-jobs)
- [Executing op jobs](https://docs.dagster.io/guides/build/jobs/op-jobs#executing-op-jobs)
