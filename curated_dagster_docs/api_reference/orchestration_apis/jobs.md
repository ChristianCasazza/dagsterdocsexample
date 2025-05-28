
On this page

A `Job` binds a `Graph` and the resources it needs to be executable.

Jobs are created by calling `GraphDefinition.to_job()` on a graph instance, or using the `job` decorator.

@dagster.job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/decorators/job_decorator.py#L146)

Creates a job with the specified parameters from the decorated graph/op invocation function.

Using this decorator allows you to build an executable job by writing a function that invokes
ops (or graphs).

Parameters:

- **(** **Callable** **\[** **...** ( _compose\_fn_) – The decorated function. The body should contain op or graph invocations. Unlike op functions, does not accept a context argument.\
\
- **Any** **\]** – The decorated function. The body should contain op or graph invocations. Unlike op functions, does not accept a context argument.

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name for the Job. Defaults to the name of the this graph.

- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – Resources that are required by this graph for execution. If not defined, io\_manager will default to filesystem.

- **config** –

Describes how the job is parameterized at runtime.

If no value is provided, then the schema for the job’s run config is a standard format based on its ops and resources.

If a dictionary is provided, then it must conform to the standard config schema, and it will be used as the job’s run config for the job whenever the job is executed. The values provided will be viewable and editable in the Dagster UI, so be careful with secrets.

If a [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig) object is provided, then it will be used directly as the run config for the job whenever the job is executed, similar to providing a dictionary.

If a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) object is provided, then the schema for the job’s run config is determined by the config mapping, and the ConfigMapping, which should return configuration in the standard format to configure the job.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that annotate the job and can be used for searching and filtering in the UI. Values that are not already strings will be serialized as JSON. If run\_tags is not set, then the content of tags will also be automatically appended to the tags of any runs of this job.

- **run\_tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that will be automatically attached to runs launched by this job. Values that are not already strings will be serialized as JSON. These tag values may be overwritten by tag values provided at invocation time. If run\_tags is set, then tags are not automatically appended to the tags of any runs of this job.

- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_ _\]_) – Arbitrary information that will be attached to the JobDefinition and be viewable in the Dagster UI. Keys must be strings, and values must be python primitive types or one of the provided MetadataValue types

- **logger\_defs** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – A dictionary of string logger identifiers to their implementations.

- **executor\_def** ( _Optional_ _\[_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_) – How this Job will be executed. Defaults to [`multiprocess_executor`](https://docs.dagster.io/api/dagster/execution#dagster.multiprocess_executor) .

- **op\_retry\_policy** ( _Optional_ _\[_ [_RetryPolicy_](https://docs.dagster.io/api/dagster/ops#dagster.RetryPolicy) _\]_) – The default retry policy for all ops in this job. Only used if retry policy is not defined on the op definition or op invocation.

- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines a discrete set of partition keys that can parameterize the job. If this argument is supplied, the config argument can’t also be supplied.

- **input\_values** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary that maps python objects to the top-level inputs of a job.


Examples:

```codeBlockLines_e6Vv
@op
def return_one():
    return 1

@op
def add_one(in1):
    return in1 + 1

@job
def job1():
    add_one(return_one())

```

`class` dagster.JobDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L88)

Defines a Dagster job.

execute\_in\_process  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L644)

Execute the Job in-process, gathering results in-memory.

The executor\_def on the Job will be ignored, and replaced with the in-process executor.
If using the default io\_manager, it will switch from filesystem to in-memory.

Parameters:

- **run\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The configuration for the run
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The instance to execute against, an ephemeral one will be used if none provided.
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The string partition key that specifies the run config to execute. Can only be used to select run config for jobs with partitioned config.
- **raise\_on\_error** ( _Optional_ _\[_ _bool_ _\]_) – Whether or not to raise exceptions when they occur. Defaults to `True`.
- **op\_selection** ( _Optional_ _\[_ _Sequence_ _\[_ _str_ _\]_ _\]_) – A list of op selection queries (including single op names) to execute. For example: \* `['some_op']`: selects `some_op` itself. \* `['*some_op']`: select `some_op` and all its ancestors (upstream dependencies). \* `['*some_op+++']`: select `some_op`, all its ancestors, and its descendants (downstream dependencies) within 3 levels down. \* `['*some_op', 'other_op_a', 'other_op_b+']`: select `some_op` and all its ancestors, `other_op_a` itself, and `other_op_b` and its direct child ops.
- **input\_values** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary that maps python objects to the top-level inputs of the job. Input values provided here will override input values that have been provided to the job directly.
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources needed if any are required. Can provide resource instances directly, or resource definitions.

Returns: [`ExecuteInProcessResult`](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult)

run\_request\_for\_partition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L958)

deprecated

This API will be removed in version 2.0.0.
Directly instantiate `RunRequest(partition_key=...)` instead..

Creates a RunRequest object for a run that processes the given partition.

Parameters:

- **partition\_key** – The key of the partition to request a run for.
- **run\_key** ( _Optional_ _\[_ _str_ _\]_) – A string key to identify this launched run. For sensors, ensures that only one run is created per run key across all sensor evaluations. For schedules, ensures that one run is created per tick, across failure recoveries. Passing in a None value means that a run will always be launched per evaluation.
- **tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A dictionary of tags (string key-value pairs) to attach to the launched run.
- **(** **Optional** **\[** **Mapping** **\[** **str** ( _run\_config_) – Configuration for the run. If the job has a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig), this value will override replace the config provided by it.\
- **Any** **\]** **\]** – Configuration for the run. If the job has a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig), this value will override replace the config provided by it.
- **current\_time** ( _Optional_ _\[_ _datetime_ _\]_) – Used to determine which time-partitions exist. Defaults to now.
- **dynamic\_partitions\_store** ( _Optional_ _\[_ _DynamicPartitionsStore_ _\]_) – The DynamicPartitionsStore object that is responsible for fetching dynamic partitions. Required when the partitions definition is a DynamicPartitionsDefinition with a name defined. Users can pass the DagsterInstance fetched via context.instance to this argument.

Returns: an object that requests a run to process the given partition.Return type: [RunRequest](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.RunRequest)

with\_hooks  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L1110)

Apply a set of hooks to all op instances within the job.

with\_top\_level\_resources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L1102)

Apply a set of resources to all op instances within the job.

`property` config\_mapping

The config mapping for the job, if it has one.

A config mapping defines a way to map a top-level config schema to run config for the job.

`property` executor\_def

Returns the default [`ExecutorDefinition`](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) for the job.

If the user has not specified an executor definition, then this will default to the
[`multi_or_in_process_executor()`](https://docs.dagster.io/api/dagster/execution#dagster.multi_or_in_process_executor). If a default is specified on the
[`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object the job was provided to, then that will be used instead.

`property` has\_specified\_executor

Returns True if this job has explicitly specified an executor, and False if the executor
was inherited through defaults or the [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object the job was provided to.

`property` has\_specified\_loggers

Returns true if the job explicitly set loggers, and False if loggers were inherited
through defaults or the [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object the job was provided to.

`property` loggers

Returns the set of LoggerDefinition objects specified on the job.

If the user has not specified a mapping of [`LoggerDefinition`](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) objects, then this
will default to the `colored_console_logger()` under the key console. If a default
is specified on the [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object the job was provided to, then that will
be used instead.

`property` partitioned\_config

The partitioned config for the job, if it has one.

A partitioned config defines a way to map partition keys to run config for the job.

`property` partitions\_def

Returns the [`PartitionsDefinition`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) for the job, if it has one.

A partitions definition defines the set of partition keys the job operates on.

`property` resource\_defs

Returns the set of ResourceDefinition objects specified on the job.

This may not be the complete set of resources required by the job, since those can also be
provided on the [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object the job may be provided to.

## Reconstructable jobs [​](https://docs.dagster.io/api/dagster/jobs\#reconstructable-jobs "Direct link to Reconstructable jobs")

`class` dagster.reconstructable [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/reconstruct.py#L333)

Create a `ReconstructableJob` from a
function that returns a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)/ [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition),
or a function decorated with [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job).

When your job must cross process boundaries, e.g., for execution on multiple nodes or
in different systems (like `dagstermill`), Dagster must know how to reconstruct the job
on the other side of the process boundary.

Passing a job created with `~dagster.GraphDefinition.to_job` to `reconstructable()`,
requires you to wrap that job’s definition in a module-scoped function, and pass that function
instead:

```codeBlockLines_e6Vv
from dagster import graph, reconstructable

@graph
def my_graph():
    ...

def define_my_job():
    return my_graph.to_job()

reconstructable(define_my_job)

```

This function implements a very conservative strategy for reconstruction, so that its behavior
is easy to predict, but as a consequence it is not able to reconstruct certain kinds of jobs
or jobs, such as those defined by lambdas, in nested scopes (e.g., dynamically within a method
call), or in interactive environments such as the Python REPL or Jupyter notebooks.

If you need to reconstruct objects constructed in these ways, you should use
`build_reconstructable_job()` instead, which allows you to
specify your own reconstruction strategy.

Examples:

```codeBlockLines_e6Vv
from dagster import job, reconstructable

@job
def foo_job():
    ...

reconstructable_foo_job = reconstructable(foo_job)

@graph
def foo():
    ...

def make_bar_job():
    return foo.to_job()

reconstructable_bar_job = reconstructable(make_bar_job)

```

dagster.build\_reconstructable\_job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/reconstruct.py#L446)

Create a `dagster._core.definitions.reconstructable.ReconstructableJob`.

When your job must cross process boundaries, e.g., for execution on multiple nodes or in
different systems (like `dagstermill`), Dagster must know how to reconstruct the job
on the other side of the process boundary.

This function allows you to use the strategy of your choice for reconstructing jobs, so
that you can reconstruct certain kinds of jobs that are not supported by
[`reconstructable()`](https://docs.dagster.io/api/dagster/execution#dagster.reconstructable), such as those defined by lambdas, in nested scopes (e.g.,
dynamically within a method call), or in interactive environments such as the Python REPL or
Jupyter notebooks.

If you need to reconstruct jobs constructed in these ways, use this function instead of
[`reconstructable()`](https://docs.dagster.io/api/dagster/execution#dagster.reconstructable).

Parameters:

- **reconstructor\_module\_name** ( _str_) – The name of the module containing the function to use to reconstruct the job.
- **reconstructor\_function\_name** ( _str_) – The name of the function to use to reconstruct the job.
- **reconstructable\_args** ( _Tuple_) – Args to the function to use to reconstruct the job. Values of the tuple must be JSON serializable.
- **reconstructable\_kwargs** ( _Dict_ _\[_ _str_ _,_ _Any_ _\]_) – Kwargs to the function to use to reconstruct the job. Values of the dict must be JSON serializable.

Examples:

```codeBlockLines_e6Vv
# module: mymodule

from dagster import JobDefinition, job, build_reconstructable_job

class JobFactory:
    def make_job(*args, **kwargs):

        @job
        def _job(...):
            ...

        return _job

def reconstruct_job(*args):
    factory = JobFactory()
    return factory.make_job(*args)

factory = JobFactory()

foo_job_args = (...,...)

foo_job_kwargs = {...:...}

foo_job = factory.make_job(*foo_job_args, **foo_job_kwargs)

reconstructable_foo_job = build_reconstructable_job(
    'mymodule',
    'reconstruct_job',
    foo_job_args,
    foo_job_kwargs,
)

```

- [Reconstructable jobs](https://docs.dagster.io/api/dagster/jobs#reconstructable-jobs)
