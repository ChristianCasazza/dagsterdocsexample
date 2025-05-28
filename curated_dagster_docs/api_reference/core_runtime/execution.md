
On this page

## Materializing Assets [​](https://docs.dagster.io/api/dagster/execution\#materializing-assets "Direct link to Materializing Assets")

dagster.materialize  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/materialize.py#L23)

Executes a single-threaded, in-process run which materializes provided assets.

By default, will materialize assets to the local filesystem.

Parameters:

- **assets** ( _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_) –

The assets to materialize.

Unless you’re using deps or non\_argument\_deps, you must also include all assets that are upstream of the assets that you want to materialize. This is because those upstream asset definitions have information that is needed to load their contents while materializing the downstream assets.

- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – The resources needed for execution. Can provide resource instances directly, or resource definitions. Note that if provided resources conflict with resources directly on assets, an error will be thrown.

- **run\_config** ( _Optional_ _\[_ _Any_ _\]_) – The run config to use for the run that materializes the assets.

- **partition\_key** – (Optional\[str\]) The string partition key that specifies the run config to execute. Can only be used to select run config for assets with partitioned config.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for the run.

- **selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) –

A sub-selection of assets to materialize.

If not provided, then all assets will be materialized.


Returns: The result of the execution.Return type: [ExecuteInProcessResult](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult)
Examples:

```codeBlockLines_e6Vv
@asset
def asset1():
    ...

@asset
def asset2(asset1):
    ...

# executes a run that materializes asset1 and then asset2
materialize([asset1, asset2])

# executes a run that materializes just asset2, loading its input from asset1
materialize([asset1, asset2], selection=[asset2])

```

dagster.materialize\_to\_memory  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/materialize.py#L116)

Executes a single-threaded, in-process run which materializes provided assets in memory.

Will explicitly use [`mem_io_manager()`](https://docs.dagster.io/api/dagster/io-managers#dagster.mem_io_manager) for all required io manager
keys. If any io managers are directly provided using the resources
argument, a [`DagsterInvariantViolationError`](https://docs.dagster.io/api/dagster/errors#dagster.DagsterInvariantViolationError) will be thrown.

Parameters:

- **assets** ( _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_) – The assets to materialize. Can also provide [`SourceAsset`](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) objects to fill dependencies for asset defs.

- **run\_config** ( _Optional_ _\[_ _Any_ _\]_) – The run config to use for the run that materializes the assets.

- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – The resources needed for execution. Can provide resource instances directly, or resource definitions. If provided resources conflict with resources directly on assets, an error will be thrown.

- **partition\_key** – (Optional\[str\]) The string partition key that specifies the run config to execute. Can only be used to select run config for assets with partitioned config.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Tags for the run.

- **selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) –

A sub-selection of assets to materialize.

If not provided, then all assets will be materialized.


Returns: The result of the execution.Return type: [ExecuteInProcessResult](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult)
Examples:

```codeBlockLines_e6Vv
@asset
def asset1():
    ...

@asset
def asset2(asset1):
    ...

# executes a run that materializes asset1 and then asset2
materialize([asset1, asset2])

# executes a run that materializes just asset1
materialize([asset1, asset2], selection=[asset1])

```

## Executing Jobs [​](https://docs.dagster.io/api/dagster/execution\#executing-jobs "Direct link to Executing Jobs")

`class` dagster.JobDefinition [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L88)

Defines a Dagster job.

execute\_in\_process [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L644)

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

run\_request\_for\_partition [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L958)

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

with\_hooks [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L1110)

Apply a set of hooks to all op instances within the job.

with\_top\_level\_resources [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/job_definition.py#L1102)

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

dagster.execute\_job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/api.py#L316)

Execute a job synchronously.

This API represents dagster’s python entrypoint for out-of-process
execution. For most testing purposes, `     execute_in_process()` will be more suitable, but when wanting to run
execution using an out-of-process executor (such as `dagster.     multiprocess_executor`), then execute\_job is suitable.

execute\_job expects a persistent [`DagsterInstance`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) for
execution, meaning the $DAGSTER\_HOME environment variable must be set.
It also expects a reconstructable pointer to a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) so
that it can be reconstructed in separate processes. This can be done by
wrapping the `JobDefinition` in a call to `dagster.     reconstructable()`.

```codeBlockLines_e6Vv
from dagster import DagsterInstance, execute_job, job, reconstructable

@job
def the_job():
    ...

instance = DagsterInstance.get()
result = execute_job(reconstructable(the_job), instance=instance)
assert result.success

```

If using the [`to_job()`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition.to_job) method to
construct the `JobDefinition`, then the invocation must be wrapped in a
module-scope function, which can be passed to `reconstructable`.

```codeBlockLines_e6Vv
from dagster import graph, reconstructable

@graph
def the_graph():
    ...

def define_job():
    return the_graph.to_job(...)

result = execute_job(reconstructable(define_job), ...)

```

Since execute\_job is potentially executing outside of the current
process, output objects need to be retrieved by use of the provided job’s
io managers. Output objects can be retrieved by opening the result of
execute\_job as a context manager.

```codeBlockLines_e6Vv
from dagster import execute_job

with execute_job(...) as result:
    output_obj = result.output_for_node("some_op")

```

`execute_job` can also be used to reexecute a run, by providing a [`ReexecutionOptions`](https://docs.dagster.io/api/dagster/execution#dagster.ReexecutionOptions) object.

```codeBlockLines_e6Vv
from dagster import ReexecutionOptions, execute_job

instance = DagsterInstance.get()

options = ReexecutionOptions.from_failure(run_id=failed_run_id, instance)
execute_job(reconstructable(job), instance, reexecution_options=options)

```

Parameters:

- **job** ( _ReconstructableJob_) – A reconstructable pointer to a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition).

- **instance** ( [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)) – The instance to execute against.

- **run\_config** ( _Optional_ _\[_ _dict_ _\]_) – The configuration that parametrizes this run, as a dict.

- **tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Arbitrary key-value pairs that will be added to run logs.

- **raise\_on\_error** ( _Optional_ _\[_ _bool_ _\]_) – Whether or not to raise exceptions when they occur. Defaults to `False`.

- **op\_selection** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) –

A list of op selection queries (including single op names) to execute. For example:
  - `['some_op']`: selects `some_op` itself.
  - `['*some_op']`: select `some_op` and all its ancestors (upstream dependencies).
  - `['*some_op+++']`: select `some_op`, all its ancestors, and its descendants (downstream dependencies) within 3 levels down.
  - `['*some_op', 'other_op_a', 'other_op_b+']`: select `some_op` and all its ancestors, `other_op_a` itself, and `other_op_b` and its direct child ops.
- **reexecution\_options** ( _Optional_ _\[_ [_ReexecutionOptions_](https://docs.dagster.io/api/dagster/execution#dagster.ReexecutionOptions) _\]_) – Reexecution options to provide to the run, if this run is intended to be a reexecution of a previous run. Cannot be used in tandem with the `op_selection` argument.


Returns: The result of job execution.Return type: [`JobExecutionResult`](https://docs.dagster.io/api/dagster/execution#dagster.JobExecutionResult)

`class` dagster.ReexecutionOptions  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/api.py#L278)

Reexecution options for python-based execution in Dagster.

Parameters:

- **parent\_run\_id** ( _str_) – The run\_id of the run to reexecute.

- **step\_selection** ( _Sequence_ _\[_ _str_ _\]_) –

The list of step selections to reexecute. Must be a subset or match of the set of steps executed in the original run. For example:
  - `['some_op']`: selects `some_op` itself.
  - `['*some_op']`: select `some_op` and all its ancestors (upstream dependencies).
  - `['*some_op+++']`: select `some_op`, all its ancestors, and its descendants (downstream dependencies) within 3 levels down.
  - `['*some_op', 'other_op_a', 'other_op_b+']`: select `some_op` and all its ancestors, `other_op_a` itself, and `other_op_b` and its direct child ops.

dagster.instance\_for\_test  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/instance_for_test.py#L16)

Creates a persistent [`DagsterInstance`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) available within a context manager.

When a context manager is opened, if no temp\_dir parameter is set, a new
temporary directory will be created for the duration of the context
manager’s opening. If the set\_dagster\_home parameter is set to True
(True by default), the $DAGSTER\_HOME environment variable will be
overridden to be this directory (or the directory passed in by temp\_dir)
for the duration of the context manager being open.

Parameters:

- **overrides** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Config to provide to instance (config format follows that typically found in an instance.yaml file).
- **set\_dagster\_home** ( _Optional_ _\[_ _bool_ _\]_) – If set to True, the $DAGSTER\_HOME environment variable will be overridden to be the directory used by this instance for the duration that the context manager is open. Upon the context manager closing, the $DAGSTER\_HOME variable will be re-set to the original value. (Defaults to True).
- **temp\_dir** ( _Optional_ _\[_ _str_ _\]_) – The directory to use for storing local artifacts produced by the instance. If not set, a temporary directory will be created for the duration of the context manager being open, and all artifacts will be torn down afterward.

## Executing Graphs [​](https://docs.dagster.io/api/dagster/execution\#executing-graphs "Direct link to Executing Graphs")

`class` dagster.GraphDefinition [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L126)

Defines a Dagster op graph.

An op graph is made up of

- Nodes, which can either be an op (the functional unit of computation), or another graph.
- Dependencies, which determine how the values produced by nodes as outputs flow from one node to another. This tells Dagster how to arrange nodes into a directed, acyclic graph (DAG) of compute.

End users should prefer the [`@graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph) decorator. GraphDefinition is generally
intended to be used by framework authors or for programatically generated graphs.

Parameters:

- **name** ( _str_) – The name of the graph. Must be unique within any [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) or [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) containing the graph.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the job.
- **node\_defs** ( _Optional_ _\[_ _Sequence_ _\[_ _NodeDefinition_ _\]_ _\]_) – The set of ops / graphs used in this graph.
- **dependencies** ( _Optional_ _\[_ _Dict_ _\[_ _Union_ _\[_ _str_ _,_ [_NodeInvocation_](https://docs.dagster.io/api/dagster/graphs#dagster.NodeInvocation) _\]_ _,_ _Dict_ _\[_ _str_ _,_ [_DependencyDefinition_](https://docs.dagster.io/api/dagster/graphs#dagster.DependencyDefinition) _\]_ _\]_ _\]_) – A structure that declares the dependencies of each op’s inputs on the outputs of other ops in the graph. Keys of the top level dict are either the string names of ops in the graph or, in the case of aliased ops, [`NodeInvocations`](https://docs.dagster.io/api/dagster/graphs#dagster.NodeInvocation). Values of the top level dict are themselves dicts, which map input names belonging to the op or aliased op to [`DependencyDefinitions`](https://docs.dagster.io/api/dagster/graphs#dagster.DependencyDefinition).
- **input\_mappings** ( _Optional_ _\[_ _Sequence_ _\[_ [_InputMapping_](https://docs.dagster.io/api/dagster/graphs#dagster.InputMapping) _\]_ _\]_) – Defines the inputs to the nested graph, and how they map to the inputs of its constituent ops.
- **output\_mappings** ( _Optional_ _\[_ _Sequence_ _\[_ [_OutputMapping_](https://docs.dagster.io/api/dagster/graphs#dagster.OutputMapping) _\]_ _\]_) – Defines the outputs of the nested graph, and how they map from the outputs of its constituent ops.
- **config** ( _Optional_ _\[_ [_ConfigMapping_](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) _\]_) – Defines the config of the graph, and how its schema maps to the config of its constituent ops.
- **tags** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Arbitrary metadata for any execution of the graph. Values that are not strings will be json encoded and must meet the criteria that json.loads(json.dumps(value)) == value. These tag values may be overwritten by tag values provided at invocation time.
- **composition\_fn** ( _Optional_ _\[_ _Callable_ _\]_) – The function that defines this graph. Used to generate code references for this graph.

Examples:

```codeBlockLines_e6Vv
@op
def return_one():
    return 1

@op
def add_one(num):
    return num + 1

graph_def = GraphDefinition(
    name='basic',
    node_defs=[return_one, add_one],
    dependencies={'add_one': {'num': DependencyDefinition('return_one')}},
)

```

alias [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L818)

Aliases the graph with a new name.

Can only be used in the context of a [`@graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph), [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job), or `@asset_graph` decorated function.

**Examples:**

```codeBlockLines_e6Vv
@job
def do_it_all():
    my_graph.alias("my_graph_alias")

```

execute\_in\_process [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L714)

Execute this graph in-process, collecting results in-memory.

Parameters:

- **run\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Run config to provide to execution. The configuration for the underlying graph should exist under the “ops” key.
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The instance to execute against, an ephemeral one will be used if none provided.
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources needed if any are required. Can provide resource instances directly, or resource definitions.
- **raise\_on\_error** ( _Optional_ _\[_ _bool_ _\]_) – Whether or not to raise exceptions when they occur. Defaults to `True`.
- **op\_selection** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – A list of op selection queries (including single op names) to execute. For example: \* `['some_op']`: selects `some_op` itself. \* `['*some_op']`: select `some_op` and all its ancestors (upstream dependencies). \* `['*some_op+++']`: select `some_op`, all its ancestors, and its descendants (downstream dependencies) within 3 levels down. \* `['*some_op', 'other_op_a', 'other_op_b+']`: select `some_op` and all its ancestors, `other_op_a` itself, and `other_op_b` and its direct child ops.
- **input\_values** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary that maps python objects to the top-level inputs of the graph.

Returns: [`ExecuteInProcessResult`](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult)

tag [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L833)

Attaches the provided tags to the graph immutably.

Can only be used in the context of a [`@graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph), [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job), or `@asset_graph` decorated function.

**Examples:**

```codeBlockLines_e6Vv
@job
def do_it_all():
    my_graph.tag({"my_tag": "my_value"})

```

to\_job [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L599)

Make this graph in to an executable Job by providing remaining components required for execution.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name for the Job. Defaults to the name of the this graph.

- **resource\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – Resources that are required by this graph for execution. If not defined, io\_manager will default to filesystem.

- **config** –

Describes how the job is parameterized at runtime.

If no value is provided, then the schema for the job’s run config is a standard format based on its ops and resources.

If a dictionary is provided, then it must conform to the standard config schema, and it will be used as the job’s run config for the job whenever the job is executed. The values provided will be viewable and editable in the Dagster UI, so be careful with secrets.

If a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) object is provided, then the schema for the job’s run config is determined by the config mapping, and the ConfigMapping, which should return configuration in the standard format to configure the job.

- **tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that annotate the job and can be used for searching and filtering in the UI. Values that are not already strings will be serialized as JSON. If run\_tags is not set, then the content of tags will also be automatically appended to the tags of any runs of this job.

- **run\_tags** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _object_ _\]_ _\]_) – A set of key-value tags that will be automatically attached to runs launched by this job. Values that are not already strings will be serialized as JSON. These tag values may be overwritten by tag values provided at invocation time. If run\_tags is set, then tags are not automatically appended to the tags of any runs of this job.

- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_ _\]_) – Arbitrary information that will be attached to the JobDefinition and be viewable in the Dagster UI. Keys must be strings, and values must be python primitive types or one of the provided MetadataValue types

- **logger\_defs** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – A dictionary of string logger identifiers to their implementations.

- **executor\_def** ( _Optional_ _\[_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_) – How this Job will be executed. Defaults to [`multi_or_in_process_executor`](https://docs.dagster.io/api/dagster/execution#dagster.multi_or_in_process_executor), which can be switched between multi-process and in-process modes of execution. The default mode of execution is multi-process.

- **op\_retry\_policy** ( _Optional_ _\[_ [_RetryPolicy_](https://docs.dagster.io/api/dagster/ops#dagster.RetryPolicy) _\]_) – The default retry policy for all ops in this job. Only used if retry policy is not defined on the op definition or op invocation.

- **partitions\_def** ( _Optional_ _\[_ [_PartitionsDefinition_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) _\]_) – Defines a discrete set of partition keys that can parameterize the job. If this argument is supplied, the config argument can’t also be supplied.

- **asset\_layer** ( _Optional_ _\[_ _AssetLayer_ _\]_) – Top level information about the assets this job will produce. Generally should not be set manually.

- **input\_values** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dictionary that maps python objects to the top-level inputs of a job.


Returns: JobDefinition

with\_hooks [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L848)

Attaches the provided hooks to the graph immutably.

Can only be used in the context of a [`@graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph), [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job), or `@asset_graph` decorated function.

**Examples:**

```codeBlockLines_e6Vv
@job
def do_it_all():
    my_graph.with_hooks({my_hook})

```

with\_retry\_policy [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/graph_definition.py#L863)

Attaches the provided retry policy to the graph immutably.

Can only be used in the context of a [`@graph`](https://docs.dagster.io/api/dagster/graphs#dagster.graph), [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job), or `@asset_graph` decorated function.

**Examples:**

```codeBlockLines_e6Vv
@job
def do_it_all():
    my_graph.with_retry_policy(RetryPolicy(max_retries=5))

```

`property` config\_mapping

The config mapping for the graph, if present.

By specifying a config mapping function, you can override the configuration for the child nodes contained within a graph.

`property` input\_mappings

Input mappings for the graph.

An input mapping is a mapping from an input of the graph to an input of a child node.

`property` name

The name of the graph.

`property` output\_mappings

Output mappings for the graph.

An output mapping is a mapping from an output of the graph to an output of a child node.

`property` tags

The tags associated with the graph.

## Execution results [​](https://docs.dagster.io/api/dagster/execution\#execution-results "Direct link to Execution results")

`class` dagster.ExecuteInProcessResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/execute_in_process_result.py#L16)

Result object returned by in-process testing APIs.

Users should not instantiate this object directly. Used for retrieving run success, events, and outputs from execution methods that return this object.

This object is returned by:

- [`dagster.GraphDefinition.execute_in_process()`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition.execute_in_process)
- [`dagster.JobDefinition.execute_in_process()`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition.execute_in_process)
- [`dagster.materialize_to_memory()`](https://docs.dagster.io/api/dagster/execution#dagster.materialize_to_memory)
- [`dagster.materialize()`](https://docs.dagster.io/api/dagster/execution#dagster.materialize)

asset\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/execute_in_process_result.py#L122)

Retrieves the value of an asset that was materialized during the execution of the job.

Parameters: **asset\_key** ( _CoercibleToAssetKey_) – The key of the asset to retrieve.Returns: The value of the retrieved asset.Return type: Any

output\_for\_node  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/execute_in_process_result.py#L107)

Retrieves output value with a particular name from the in-process run of the job.

Parameters:

- **node\_str** ( _str_) – Name of the op/graph whose output should be retrieved. If the intended graph/op is nested within another graph, the syntax is outer\_graph.inner\_node.
- **output\_name** ( _Optional_ _\[_ _str_ _\]_) – Name of the output on the op/graph to retrieve. Defaults to result, the default output name in dagster.

Returns: The value of the retrieved output.Return type: Any

output\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/execute_in_process_result.py#L139)

Retrieves output of top-level job, if an output is returned.

Parameters: **output\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the output to retrieve. Defaults to result,
the default output name in dagster.Returns: The value of the retrieved output.Return type: Any

`property` all\_events

All dagster events emitted during execution.

Type: List\[ [DagsterEvent](https://docs.dagster.io/api/dagster/execution#dagster.DagsterEvent)\]

`property` dagster\_run

The Dagster run that was executed.

Type: [DagsterRun](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun)

`property` job\_def

The job definition that was executed.

Type: [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)

`property` run\_id

The run ID of the executed [`DagsterRun`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun).

Type: str

`class` dagster.JobExecutionResult  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/job_execution_result.py#L18)

Result object returned by [`dagster.execute_job()`](https://docs.dagster.io/api/dagster/execution#dagster.execute_job).

Used for retrieving run success, events, and outputs from execute\_job.
Users should not directly instantiate this class.

Events and run information can be retrieved off of the object directly. In
order to access outputs, the ExecuteJobResult object needs to be opened
as a context manager, which will re-initialize the resources from
execution.

output\_for\_node  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/job_execution_result.py#L92)

Retrieves output value with a particular name from the run of the job.

In order to use this method, the ExecuteJobResult object must be opened as a context manager. If this method is used without opening the context manager, it will result in a [`DagsterInvariantViolationError`](https://docs.dagster.io/api/dagster/errors#dagster.DagsterInvariantViolationError).

Parameters:

- **node\_str** ( _str_) – Name of the op/graph whose output should be retrieved. If the intended graph/op is nested within another graph, the syntax is outer\_graph.inner\_node.
- **output\_name** ( _Optional_ _\[_ _str_ _\]_) – Name of the output on the op/graph to retrieve. Defaults to result, the default output name in dagster.

Returns: The value of the retrieved output.Return type: Any

output\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/job_execution_result.py#L77)

Retrieves output of top-level job, if an output is returned.

In order to use this method, the ExecuteJobResult object must be opened as a context manager. If this method is used without opening the context manager, it will result in a [`DagsterInvariantViolationError`](https://docs.dagster.io/api/dagster/errors#dagster.DagsterInvariantViolationError). If the top-level job has no output, calling this method will also result in a [`DagsterInvariantViolationError`](https://docs.dagster.io/api/dagster/errors#dagster.DagsterInvariantViolationError).

Parameters: **output\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the output to retrieve. Defaults to result,
the default output name in dagster.Returns: The value of the retrieved output.Return type: Any

`property` all\_events

List of all events yielded by the job execution.

Type: Sequence\[ [DagsterEvent](https://docs.dagster.io/api/dagster/execution#dagster.DagsterEvent)\]

`property` dagster\_run

The Dagster run that was executed.

Type: [DagsterRun](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun)

`property` job\_def

The job definition that was executed.

Type: [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)

`property` run\_id

The id of the Dagster run that was executed.

Type: str

`class` dagster.DagsterEvent  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/events/__init__.py#L435)

Events yielded by op and job execution.

Users should not instantiate this class.

Parameters:

- **event\_type\_value** ( _str_) – Value for a DagsterEventType.
- **job\_name** ( _str_)
- **node\_handle** ( _NodeHandle_)
- **step\_kind\_value** ( _str_) – Value for a StepKind.
- **logging\_tags** ( _Dict_ _\[_ _str_ _,_ _str_ _\]_)
- **event\_specific\_data** ( _Any_) – Type must correspond to event\_type\_value.
- **message** ( _str_)
- **pid** ( _int_)
- **step\_key** ( _Optional_ _\[_ _str_ _\]_) – DEPRECATED

`property` asset\_key

For events that correspond to a specific asset\_key / partition
(ASSET\_MATERIALIZTION, ASSET\_OBSERVATION, ASSET\_MATERIALIZATION\_PLANNED), returns that
asset key. Otherwise, returns None.

Type: Optional\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)\]

`property` event\_type

The type of this event.

Type: [DagsterEventType](https://docs.dagster.io/api/dagster/execution#dagster.DagsterEventType)

`property` is\_asset\_materialization\_planned

If this event is of type ASSET\_MATERIALIZATION\_PLANNED.

Type: bool

`property` is\_asset\_observation

If this event is of type ASSET\_OBSERVATION.

Type: bool

`property` is\_engine\_event

If this event is of type ENGINE\_EVENT.

Type: bool

`property` is\_expectation\_result

If this event is of type STEP\_EXPECTATION\_RESULT.

Type: bool

`property` is\_failure

If this event represents the failure of a run or step.

Type: bool

`property` is\_handled\_output

If this event is of type HANDLED\_OUTPUT.

Type: bool

`property` is\_hook\_event

If this event relates to the execution of a hook.

Type: bool

`property` is\_loaded\_input

If this event is of type LOADED\_INPUT.

Type: bool

`property` is\_resource\_init\_failure

If this event is of type RESOURCE\_INIT\_FAILURE.

Type: bool

`property` is\_step\_event

If this event relates to a specific step.

Type: bool

`property` is\_step\_failure

If this event is of type STEP\_FAILURE.

Type: bool

`property` is\_step\_materialization

If this event is of type ASSET\_MATERIALIZATION.

Type: bool

`property` is\_step\_restarted

If this event is of type STEP\_RESTARTED.

Type: bool

`property` is\_step\_skipped

If this event is of type STEP\_SKIPPED.

Type: bool

`property` is\_step\_start

If this event is of type STEP\_START.

Type: bool

`property` is\_step\_success

If this event is of type STEP\_SUCCESS.

Type: bool

`property` is\_step\_up\_for\_retry

If this event is of type STEP\_UP\_FOR\_RETRY.

Type: bool

`property` is\_successful\_output

If this event is of type STEP\_OUTPUT.

Type: bool

`property` partition

For events that correspond to a specific asset\_key / partition
(ASSET\_MATERIALIZTION, ASSET\_OBSERVATION, ASSET\_MATERIALIZATION\_PLANNED), returns that
partition. Otherwise, returns None.

Type: Optional\[ [AssetKey](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)\]

`class` dagster.DagsterEventType  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/events/__init__.py#L102)

The types of events that may be yielded by op and job execution.

## Reconstructable jobs [​](https://docs.dagster.io/api/dagster/execution\#reconstructable-jobs "Direct link to Reconstructable jobs")

`class` dagster.reconstructable  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/reconstruct.py#L333)

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

## Executors [​](https://docs.dagster.io/api/dagster/execution\#executors "Direct link to Executors")

dagster.multi\_or\_in\_process\_executor ExecutorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/executor_definition.py#L472)

The default executor for a job.

This is the executor available by default on a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)
that does not provide custom executors. This executor has a multiprocessing-enabled mode, and a
single-process mode. By default, multiprocessing mode is enabled. Switching between multiprocess
mode and in-process mode can be achieved via config.

```codeBlockLines_e6Vv
execution:
  config:
    multiprocess:

execution:
  config:
    in_process:

```

When using the multiprocess mode, `max_concurrent` and `retries` can also be configured.

```codeBlockLines_e6Vv
execution:
  config:
    multiprocess:
      max_concurrent: 4
      retries:
        enabled:

```

The `max_concurrent` arg is optional and tells the execution engine how many processes may run
concurrently. By default, or if you set `max_concurrent` to be 0, this is the return value of
`python:multiprocessing.cpu_count()`.

When using the in\_process mode, then only retries can be configured.

Execution priority can be configured using the `dagster/priority` tag via op metadata,
where the higher the number the higher the priority. 0 is the default and both positive
and negative numbers can be used.

dagster.in\_process\_executor ExecutorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/executor_definition.py#L288)

The in-process executor executes all steps in a single process.

To select it, include the following top-level fragment in config:

```codeBlockLines_e6Vv
execution:
  in_process:

```

Execution priority can be configured using the `dagster/priority` tag via op metadata,
where the higher the number the higher the priority. 0 is the default and both positive
and negative numbers can be used.

dagster.multiprocess\_executor ExecutorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/executor_definition.py#L399)

The multiprocess executor executes each step in an individual process.

Any job that does not specify custom executors will use the multiprocess\_executor by default.
To configure the multiprocess executor, include a fragment such as the following in your run
config:

```codeBlockLines_e6Vv
execution:
  config:
    multiprocess:
      max_concurrent: 4

```

The `max_concurrent` arg is optional and tells the execution engine how many processes may run
concurrently. By default, or if you set `max_concurrent` to be None or 0, this is the return value of
`python:multiprocessing.cpu_count()`.

Execution priority can be configured using the `dagster/priority` tag via op metadata,
where the higher the number the higher the priority. 0 is the default and both positive
and negative numbers can be used.

## Contexts [​](https://docs.dagster.io/api/dagster/execution\#contexts "Direct link to Contexts")

`class` dagster.AssetExecutionContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L86)add\_asset\_metadata  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L468)

Add metadata to an asset materialization event. This metadata will be
available in the Dagster UI.

Parameters:

- **metadata** ( _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) – The metadata to add to the asset materialization event.
- **asset\_key** ( _Optional_ _\[_ _CoercibleToAssetKey_ _\]_) – The asset key to add metadata to. Does not need to be provided if only one asset is currently being materialized.
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The partition key to add metadata to, if applicable. Should not be provided on non-partitioned assets. If not provided on a partitioned asset, the metadata will be added to all partitions of the asset currently being materialized.

Examples:

Adding metadata to the asset materialization event for a single asset:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def my_asset(context):
    # Add metadata
    context.add_asset_metadata({"key": "value"})

```

Adding metadata to the asset materialization event for a particular partition of a partitioned asset:

```codeBlockLines_e6Vv
import dagster as dg

@dg.asset(partitions_def=dg.StaticPartitionsDefinition(["a", "b"]))
def my_asset(context):
    # Adds metadata to all partitions currently being materialized, since no
    # partition is specified.
    context.add_asset_metadata({"key": "value"})

    for partition_key in context.partition_keys:
        # Add metadata only to the event for partition "a"
        if partition_key == "a":
            context.add_asset_metadata({"key": "value"}, partition_key=partition_key)

```

Adding metadata to the asset materialization event for a particular asset in a multi-asset.

```codeBlockLines_e6Vv
import dagster as dg

@dg.multi_asset(specs=[dg.AssetSpec("asset1"), dg.AssetSpec("asset2")])
def my_multi_asset(context):
    # Add metadata to the materialization event for "asset1"
    context.add_asset_metadata({"key": "value"}, asset_key="asset1")

    # THIS line will fail since asset key is not specified:
    context.add_asset_metadata({"key": "value"})

```

add\_output\_metadata  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L454)

Add metadata to one of the outputs of an op.

This can be invoked multiple times per output in the body of an op. If the same key is
passed multiple times, the value associated with the last call will be used.

Parameters:

- **metadata** ( _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) – The metadata to attach to the output
- **output\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the output to attach metadata to. If there is only one output on the op, then this argument does not need to be provided. The metadata will automatically be attached to the only output.
- **mapping\_key** ( _Optional_ _\[_ _str_ _\]_) – The mapping key of the output to attach metadata to. If the output is not dynamic, this argument does not need to be provided.

**Examples:**

```codeBlockLines_e6Vv
from dagster import Out, op
from typing import Tuple

@op
def add_metadata(context):
    context.add_output_metadata({"foo", "bar"})
    return 5 # Since the default output is called "result", metadata will be attached to the output "result".

@op(out={"a": Out(), "b": Out()})
def add_metadata_two_outputs(context) -> Tuple[str, int]:
    context.add_output_metadata({"foo": "bar"}, output_name="b")
    context.add_output_metadata({"baz": "bat"}, output_name="a")

    return ("dog", 5)

```

asset\_key\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L339)

Return the AssetKey for the corresponding input.

asset\_key\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L329)

Return the AssetKey for the corresponding output.

asset\_partition\_key\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L417)

Returns the partition key of the upstream asset corresponding to the given input.

Parameters: **input\_name** ( _str_) – The name of the input to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_for_input("self_dependent_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-20"

```

asset\_partition\_key\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L212)

deprecated

This API will be removed in version a future release.
You have called the deprecated method asset\_partition\_key\_for\_output on AssetExecutionContext. Use context.partition\_key instead..

Returns the asset partition key for the given output.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_for_output("first_asset"))
    context.log.info(context.asset_partition_key_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"
#   "2023-08-21"

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

```

asset\_partition\_key\_range\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L412)

Return the PartitionKeyRange for the corresponding input. Errors if the asset depends on a
non-contiguous chunk of the input.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_key_range_for_input` to get the range of partitions keys of the input that
are relevant to that backfill.

Parameters: **input\_name** ( _str_) – The name of the input to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_range_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_range_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-20", end="2023-08-24")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_range_for_input("self_dependent_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-20", end="2023-08-24")

```

asset\_partition\_key\_range\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L224)

deprecated

This API will be removed in version a future release.
You have called the deprecated method asset\_partition\_key\_range\_for\_output on AssetExecutionContext. Use context.partition\_key\_range instead..

Return the PartitionKeyRange for the corresponding output. Errors if the run is not partitioned.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_key_range_for_output` to get all of the partitions being materialized
by the backfill.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition key range for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_range_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_range_for_output("first_asset"))
    context.log.info(context.asset_partition_key_range_for_output("second_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_range_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

```

asset\_partition\_keys\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L427)

Returns a list of the partition keys of the upstream asset corresponding to the
given input.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_keys_for_input` to get all of the partition keys of the input that
are relevant to that backfill.

Parameters: **input\_name** ( _str_) – The name of the input to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_keys_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_keys_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-20", "2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24"]

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_keys_for_input("self_dependent_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-20", "2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24"]

```

asset\_partition\_keys\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L238)

deprecated

This API will be removed in version a future release.
You have called the deprecated method asset\_partition\_keys\_for\_output on AssetExecutionContext. Use context.partition\_keys instead..

Returns a list of the partition keys for the given output.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_keys_for_output` to get all of the partitions being materialized
by the backfill.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition keys for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_keys_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_keys_for_output("first_asset"))
    context.log.info(context.asset_partition_keys_for_output("second_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_keys_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

```

asset\_partitions\_def\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L422)

The PartitionsDefinition on the upstream asset corresponding to this input.

Parameters: **input\_name** ( _str_) – The name of the input to get the PartitionsDefinition for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def upstream_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_def_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")

```

asset\_partitions\_def\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L232)

deprecated

This API will be removed in version a future release.
You have called the deprecated method asset\_partitions\_def\_for\_output on AssetExecutionContext. Use context.assets\_def.partitions\_def instead..

The PartitionsDefinition on the asset corresponding to this output.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the PartitionsDefinition for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_def_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_def_for_output("first_asset"))
    context.log.info(context.asset_partitions_def_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")
#   DailyPartitionsDefinition("2023-08-20")

```

asset\_partitions\_time\_window\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L432)

The time window for the partitions of the input asset.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partitions_time_window_for_input` to get the time window of the input that
are relevant to that backfill.

Raises an error if either of the following are true:

- The input asset has no partitioning.
- The input asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

Parameters: **input\_name** ( _str_) – The name of the input to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_time_window_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_time_window_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-21")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partitions_time_window_for_input("self_dependent_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-21")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-25")

```

asset\_partitions\_time\_window\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L218)

deprecated

This API will be removed in version a future release.
You have called the deprecated method asset\_partitions\_time\_window\_for\_output on AssetExecutionContext. Use context.partition\_time\_window instead..

The time window for the partitions of the output asset.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partitions_time_window_for_output` to get the TimeWindow of all of the partitions
being materialized by the backfill.

Raises an error if either of the following are true:

- The output asset has no partitioning.
- The output asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_time_window_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_time_window_for_output("first_asset"))
    context.log.info(context.asset_partitions_time_window_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partitions_time_window_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

```

get\_asset\_provenance  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L560)

Return the provenance information for the most recent materialization of an asset.

Parameters: **asset\_key** ( [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)) – Key of the asset for which to retrieve provenance.Returns:
Provenance information for the most recent
materialization of the asset. Returns None if the asset was never materialized or
the materialization record is too old to contain provenance information.

Return type: Optional\[DataProvenance\]

get\_mapping\_key  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L269)

deprecated

This API will be removed in version a future release.
You have called the deprecated method get\_mapping\_key on AssetExecutionContext. Use context.op\_execution\_context.get\_mapping\_key instead..

Which mapping\_key this execution is for if downstream of a DynamicOutput, otherwise None.

get\_tag  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L202)

deprecated

This API will be removed in version a future release.
You have called the deprecated method get\_tag on AssetExecutionContext. Use context.run.tags.get(key) instead..

Get a logging tag.

Parameters: **key** ( _tag_) – The tag to get.Returns: The value of the tag, if present.Return type: Optional\[str\]

has\_tag  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L196)

deprecated

This API will be removed in version a future release.
You have called the deprecated method has\_tag on AssetExecutionContext. Use key in context.run.tags instead..

Check if a logging tag is set.

Parameters: **key** ( _str_) – The tag to check.Returns: Whether the tag is set.Return type: bool

log\_event  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L447)

Log an AssetMaterialization, AssetObservation, or ExpectationResult from within the body of an op.

Events logged with this method will appear in the list of DagsterEvents, as well as the event log.

Parameters: **event** ( _Union_ _\[_ [_AssetMaterialization_](https://docs.dagster.io/api/dagster/ops#dagster.AssetMaterialization) _,_ [_AssetObservation_](https://docs.dagster.io/api/dagster/assets#dagster.AssetObservation) _,_ [_ExpectationResult_](https://docs.dagster.io/api/dagster/ops#dagster.ExpectationResult) _\]_) – The event to log.
**Examples:**

```codeBlockLines_e6Vv
from dagster import op, AssetMaterialization

@op
def log_materialization(context):
    context.log_event(AssetMaterialization("foo"))

```

output\_for\_asset\_key  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/asset_execution_context.py#L334)

Return the output name for the corresponding asset key.

`property` asset\_key

The AssetKey for the current asset. In a multi\_asset, use asset\_key\_for\_output instead.

`property` asset\_partition\_key\_range

deprecated

This API will be removed in version 2.0.
Use `partition_key_range` instead..

The range of partition keys for the current run.

If run is for a single partition key, return a PartitionKeyRange with the same start and
end. Raises an error if the current run is not a partitioned run.

`property` assets\_def

The backing AssetsDefinition for what is currently executing, errors if not available.

`property` has\_assets\_def

If there is a backing AssetsDefinition for what is currently executing.

`property` has\_partition\_key

Whether the current run targets a single partition.

`property` has\_partition\_key\_range

Whether the current run targets a range of partitions.

`property` instance

The current Dagster instance.

Type: [DagsterInstance](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)

`property` job\_def

The definition for the currently executing job. Information like the job name, and job tags
can be found on the JobDefinition.
Returns: JobDefinition.

`property` job\_name

The name of the currently executing pipeline.

Type: str

`property` log

The log manager available in the execution context. Logs will be viewable in the Dagster UI.
Returns: DagsterLogManager.

Example:

```codeBlockLines_e6Vv
@asset
def logger(context):
    context.log.info("Info level message")

```

`property` op\_config

deprecated

This API will be removed in version a future release.
You have called the deprecated method op\_config on AssetExecutionContext. Use context.op\_execution\_context.op\_config instead..

The parsed config specific to this op.

Type: Any

`property` op\_def

The current op definition.

Type: [OpDefinition](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition)

`property` partition\_key

The partition key for the current run.

Raises an error if the current run is not a partitioned run. Or if the current run is operating
over a range of partitions (ie. a backfill of several partitions executed in a single run).

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_key)

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

```

`property` partition\_key\_range

The range of partition keys for the current run.

If run is for a single partition key, returns a PartitionKeyRange with the same start and
end. Raises an error if the current run is not a partitioned run.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_key_range)

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

```

`property` partition\_keys

Returns a list of the partition keys for the current run.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `partition_keys` to get all of the partitions being materialized
by the backfill.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(partitions_def=partitions_def)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.partition_keys)

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

```

`property` partition\_time\_window

The partition time window for the current run.

Raises an error if the current run is not a partitioned run, or if the job’s partition
definition is not a TimeWindowPartitionsDefinition.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

```

`property` pdb

Gives access to pdb debugging from within the asset. Materializing the asset via the
Dagster UI or CLI will enter the pdb debugging context in the process used to launch the UI or
run the CLI.

Returns: dagster.utils.forked\_pdb.ForkedPdb

Example:

```codeBlockLines_e6Vv
@asset
def debug(context):
    context.pdb.set_trace()

```

`property` resources

The currently available resources.

Type: Resources

`property` selected\_asset\_check\_keys

Get the asset check keys that correspond to the current selection of assets this execution is expected to materialize.

`property` selected\_asset\_keys

Get the set of AssetKeys this execution is expected to materialize.

`property` selected\_output\_names

deprecated

This API will be removed in version a future release.
You have called the deprecated method selected\_output\_names on AssetExecutionContext. Use context.op\_execution\_context.selected\_output\_names instead..

Get the output names that correspond to the current selection of assets this execution is expected to materialize.

`class` dagster.OpExecutionContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L93)

The `context` object that can be made available as the first argument to the function
used for computing an op or asset.

This context object provides system information such as resources, config, and logging.

To construct an execution context for testing purposes, use [`dagster.build_op_context()`](https://docs.dagster.io/api/dagster/execution#dagster.build_op_context).

Example:

```codeBlockLines_e6Vv
from dagster import op, OpExecutionContext

@op
def hello_world(context: OpExecutionContext):
    context.log.info("Hello, world!")

```

add\_output\_metadata  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L456)

Add metadata to one of the outputs of an op.

This can be invoked multiple times per output in the body of an op. If the same key is
passed multiple times, the value associated with the last call will be used.

Parameters:

- **metadata** ( _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) – The metadata to attach to the output
- **output\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the output to attach metadata to. If there is only one output on the op, then this argument does not need to be provided. The metadata will automatically be attached to the only output.
- **mapping\_key** ( _Optional_ _\[_ _str_ _\]_) – The mapping key of the output to attach metadata to. If the output is not dynamic, this argument does not need to be provided.

**Examples:**

```codeBlockLines_e6Vv
from dagster import Out, op
from typing import Tuple

@op
def add_metadata(context):
    context.add_output_metadata({"foo", "bar"})
    return 5 # Since the default output is called "result", metadata will be attached to the output "result".

@op(out={"a": Out(), "b": Out()})
def add_metadata_two_outputs(context) -> Tuple[str, int]:
    context.add_output_metadata({"foo": "bar"}, output_name="b")
    context.add_output_metadata({"baz": "bat"}, output_name="a")

    return ("dog", 5)

```

asset\_key\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L616)

Return the AssetKey for the corresponding input.

asset\_key\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L596)

Return the AssetKey for the corresponding output.

asset\_partition\_key\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L888)

Returns the partition key of the upstream asset corresponding to the given input.

Parameters: **input\_name** ( _str_) – The name of the input to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_for_input("self_dependent_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-20"

```

asset\_partition\_key\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L627)

deprecated

This API will be removed in version 2.0.
Use `partition_key` instead..

Returns the asset partition key for the given output.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_for_output("first_asset"))
    context.log.info(context.asset_partition_key_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"
#   "2023-08-21"

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

```

asset\_partition\_key\_range\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L825)

Return the PartitionKeyRange for the corresponding input. Errors if the asset depends on a
non-contiguous chunk of the input.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_key_range_for_input` to get the range of partitions keys of the input that
are relevant to that backfill.

Parameters: **input\_name** ( _str_) – The name of the input to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_range_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_key_range_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-20", end="2023-08-24")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_range_for_input("self_dependent_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-20", end="2023-08-24")

```

asset\_partition\_key\_range\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L761)

deprecated

This API will be removed in version 2.0.
Use `partition_key_range` instead..

Return the PartitionKeyRange for the corresponding output. Errors if the run is not partitioned.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_key_range_for_output` to get all of the partitions being materialized
by the backfill.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition key range for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_range_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_key_range_for_output("first_asset"))
    context.log.info(context.asset_partition_key_range_for_output("second_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_key_range_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

```

asset\_partition\_keys\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L1083)

Returns a list of the partition keys of the upstream asset corresponding to the
given input.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_keys_for_input` to get all of the partition keys of the input that
are relevant to that backfill.

Parameters: **input\_name** ( _str_) – The name of the input to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_keys_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partition_keys_for_input("upstream_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-20", "2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24"]

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_keys_for_input("self_dependent_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-20", "2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24"]

```

asset\_partition\_keys\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L1019)

deprecated

This API will be removed in version 2.0.
Use `partition_keys` instead..

Returns a list of the partition keys for the given output.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partition_keys_for_output` to get all of the partitions being materialized
by the backfill.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the partition keys for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_keys_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partition_keys_for_output("first_asset"))
    context.log.info(context.asset_partition_keys_for_output("second_asset"))

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partition_keys_for_output())

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

```

asset\_partitions\_def\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L981)

The PartitionsDefinition on the upstream asset corresponding to this input.

Parameters: **input\_name** ( _str_) – The name of the input to get the PartitionsDefinition for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def upstream_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_def_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")

```

asset\_partitions\_def\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L931)

The PartitionsDefinition on the asset corresponding to this output.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the PartitionsDefinition for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_def_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_def_for_output("first_asset"))
    context.log.info(context.asset_partitions_def_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   DailyPartitionsDefinition("2023-08-20")
#   DailyPartitionsDefinition("2023-08-20")

```

asset\_partitions\_time\_window\_for\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L1148)

The time window for the partitions of the input asset.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partitions_time_window_for_input` to get the time window of the input that
are relevant to that backfill.

Raises an error if either of the following are true:

- The input asset has no partitioning.
- The input asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

Parameters: **input\_name** ( _str_) – The name of the input to get the partition key for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def upstream_asset():
    ...

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_time_window_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    ins={
        "upstream_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
    partitions_def=partitions_def,
)
def another_asset(context: AssetExecutionContext, upstream_asset):
    context.log.info(context.asset_partitions_time_window_for_input("upstream_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-21")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partitions_time_window_for_input("self_dependent_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-21")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-20", "2023-08-25")

```

asset\_partitions\_time\_window\_for\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L685)

deprecated

This API will be removed in version 2.0.
Use `partition_time_window` instead..

The time window for the partitions of the output asset.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `asset_partitions_time_window_for_output` to get the TimeWindow of all of the partitions
being materialized by the backfill.

Raises an error if either of the following are true:

- The output asset has no partitioning.
- The output asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

Parameters: **output\_name** ( _str_) – For assets defined with the `@asset` decorator, the name of the output
will be automatically provided. For assets defined with `@multi_asset`, `output_name`
should be the op output associated with the asset key (as determined by AssetOut)
to get the time window for.
Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_time_window_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

@multi_asset(
    outs={
        "first_asset": AssetOut(key=["my_assets", "first_asset"]),
        "second_asset": AssetOut(key=["my_assets", "second_asset"])
    }
    partitions_def=partitions_def,
)
def a_multi_asset(context: AssetExecutionContext):
    context.log.info(context.asset_partitions_time_window_for_output("first_asset"))
    context.log.info(context.asset_partitions_time_window_for_output("second_asset"))

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")
#   TimeWindow("2023-08-21", "2023-08-26")

@asset(
    partitions_def=partitions_def,
    ins={
        "self_dependent_asset": AssetIn(partition_mapping=TimeWindowPartitionMapping(start_offset=-1, end_offset=-1))
    }
)
def self_dependent_asset(context: AssetExecutionContext, self_dependent_asset):
    context.log.info(context.asset_partitions_time_window_for_output())

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-26")

```

get\_asset\_provenance  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L1224)

Return the provenance information for the most recent materialization of an asset.

Parameters: **asset\_key** ( [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey)) – Key of the asset for which to retrieve provenance.Returns:
Provenance information for the most recent
materialization of the asset. Returns None if the asset was never materialized or
the materialization record is too old to contain provenance information.

Return type: Optional\[DataProvenance\]

get\_mapping\_key  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L529)

Which mapping\_key this execution is for if downstream of a DynamicOutput, otherwise None.

get\_tag  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L391)

Get a logging tag.

Parameters: **key** ( _tag_) – The tag to get.Returns: The value of the tag, if present.Return type: Optional\[str\]

has\_tag  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L379)

Check if a logging tag is set.

Parameters: **key** ( _str_) – The tag to check.Returns: Whether the tag is set.Return type: bool

log\_event  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L420)

Log an AssetMaterialization, AssetObservation, or ExpectationResult from within the body of an op.

Events logged with this method will appear in the list of DagsterEvents, as well as the event log.

Parameters: **event** ( _Union_ _\[_ [_AssetMaterialization_](https://docs.dagster.io/api/dagster/ops#dagster.AssetMaterialization) _,_ [_AssetObservation_](https://docs.dagster.io/api/dagster/assets#dagster.AssetObservation) _,_ [_ExpectationResult_](https://docs.dagster.io/api/dagster/ops#dagster.ExpectationResult) _\]_) – The event to log.
**Examples:**

```codeBlockLines_e6Vv
from dagster import op, AssetMaterialization

@op
def log_materialization(context):
    context.log_event(AssetMaterialization("foo"))

```

output\_for\_asset\_key  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/op_execution_context.py#L607)

Return the output name for the corresponding asset key.

`property` asset\_key

The AssetKey for the current asset. In a multi\_asset, use asset\_key\_for\_output instead.

`property` asset\_partition\_key\_range

deprecated

This API will be removed in version 2.0.
Use `partition_key_range` instead..

The range of partition keys for the current run.

If run is for a single partition key, return a PartitionKeyRange with the same start and
end. Raises an error if the current run is not a partitioned run.

`property` assets\_def

The backing AssetsDefinition for what is currently executing, errors if not available.

`property` has\_assets\_def

If there is a backing AssetsDefinition for what is currently executing.

`property` has\_partition\_key

Whether the current run targets a single partition.

`property` has\_partition\_key\_range

Whether the current run targets a range of partitions.

`property` has\_partitions

Whether the current run is a partitioned run.

`property` instance

The current Dagster instance.

Type: [DagsterInstance](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance)

`property` job\_def

The currently executing job.

Type: [JobDefinition](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)

`property` job\_name

The name of the currently executing pipeline.

Type: str

`property` log

The log manager available in the execution context.

Type: [DagsterLogManager](https://docs.dagster.io/api/dagster/loggers#dagster.DagsterLogManager)

`property` op\_config

The parsed config specific to this op.

Type: Any

`property` op\_def

The current op definition.

Type: [OpDefinition](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition)

`property` partition\_key

The partition key for the current run.

Raises an error if the current run is not a partitioned run. Or if the current run is operating
over a range of partitions (ie. a backfill of several partitions executed in a single run).

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_key)

# materializing the 2023-08-21 partition of this asset will log:
#   "2023-08-21"

```

`property` partition\_key\_range

The range of partition keys for the current run.

If run is for a single partition key, returns a PartitionKeyRange with the same start and
end. Raises an error if the current run is not a partitioned run.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_key_range)

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   PartitionKeyRange(start="2023-08-21", end="2023-08-25")

```

`property` partition\_keys

Returns a list of the partition keys for the current run.

If you want to write your asset to support running a backfill of several partitions in a single run,
you can use `partition_keys` to get all of the partitions being materialized
by the backfill.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(partitions_def=partitions_def)
def an_asset(context: AssetExecutionContext):
    context.log.info(context.partition_keys)

# running a backfill of the 2023-08-21 through 2023-08-25 partitions of this asset will log:
#   ["2023-08-21", "2023-08-22", "2023-08-23", "2023-08-24", "2023-08-25"]

```

`property` partition\_time\_window

The partition time window for the current run.

Raises an error if the current run is not a partitioned run, or if the job’s partition
definition is not a TimeWindowPartitionsDefinition.

Examples:

```codeBlockLines_e6Vv
partitions_def = DailyPartitionsDefinition("2023-08-20")

@asset(
    partitions_def=partitions_def
)
def my_asset(context: AssetExecutionContext):
    context.log.info(context.partition_time_window)

# materializing the 2023-08-21 partition of this asset will log:
#   TimeWindow("2023-08-21", "2023-08-22")

```

`property` pdb

Gives access to pdb debugging from within the op.

Example:

```codeBlockLines_e6Vv
@op
def debug(context):
    context.pdb.set_trace()

```

Type: dagster.utils.forked\_pdb.ForkedPdb

`property` resources

The currently available resources.

Type: Resources

`property` retry\_number

Which retry attempt is currently executing i.e. 0 for initial attempt, 1 for first retry, etc.

`property` run

The current run.

Type: [DagsterRun](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRun)

`property` run\_config

The run config for the current execution.

Type: dict

`property` run\_id

The id of the current execution’s run.

Type: str

`property` selected\_asset\_check\_keys

Get the asset check keys that correspond to the current selection of assets this execution is expected to materialize.

`property` selected\_asset\_keys

Get the set of AssetKeys this execution is expected to materialize.

`property` selected\_output\_names

Get the output names that correspond to the current selection of assets this execution is expected to materialize.

dagster.build\_op\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/invocation.py#L871)

Builds op execution context from provided parameters.

`build_op_context` can be used as either a function or context manager. If there is a
provided resource that is a context manager, then `build_op_context` must be used as a
context manager. This function can be used to provide the context argument when directly
invoking a op.

Parameters:

- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources to provide to the context. These can be either values or resource definitions.
- **op\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The config to provide to the op.
- **resources\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The config to provide to the resources.
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured for the context. Defaults to DagsterInstance.ephemeral().
- **mapping\_key** ( _Optional_ _\[_ _str_ _\]_) – A key representing the mapping key from an upstream dynamic output. Can be accessed using `context.get_mapping_key()`.
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – String value representing partition key to execute with.
- **partition\_key\_range** ( _Optional_ _\[_ [_PartitionKeyRange_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionKeyRange) _\]_) – Partition key range to execute with.
- **run\_tags** – Optional\[Mapping\[str, str\]\]: The tags for the executing run.
- **event\_loop** – Optional\[AbstractEventLoop\]: An event loop for handling resources with async context managers.

Examples:

```codeBlockLines_e6Vv
context = build_op_context()
op_to_invoke(context)

with build_op_context(resources={"foo": context_manager_resource}) as context:
    op_to_invoke(context)

```

dagster.build\_asset\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/invocation.py#L938)

Builds asset execution context from provided parameters.

`build_asset_context` can be used as either a function or context manager. If there is a
provided resource that is a context manager, then `build_asset_context` must be used as a
context manager. This function can be used to provide the context argument when directly
invoking an asset.

Parameters:

- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources to provide to the context. These can be either values or resource definitions.
- **resources\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The config to provide to the resources.
- **asset\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The config to provide to the asset.
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured for the context. Defaults to DagsterInstance.ephemeral().
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – String value representing partition key to execute with.
- **partition\_key\_range** ( _Optional_ _\[_ [_PartitionKeyRange_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionKeyRange) _\]_) – Partition key range to execute with.
- **run\_tags** – Optional\[Mapping\[str, str\]\]: The tags for the executing run.
- **event\_loop** – Optional\[AbstractEventLoop\]: An event loop for handling resources with async context managers.

Examples:

```codeBlockLines_e6Vv
context = build_asset_context()
asset_to_invoke(context)

with build_asset_context(resources={"foo": context_manager_resource}) as context:
    asset_to_invoke(context)

```

`class` dagster.TypeCheckContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/system.py#L1313)

The `context` object available to a type check function on a DagsterType.

`property` log

Centralized log dispatch from user code.

`property` resources

An object whose attributes contain the resources available to this op.

`property` run\_id

The id of this job run.

## Job configuration [​](https://docs.dagster.io/api/dagster/execution\#job-configuration "Direct link to Job configuration")

dagster.validate\_run\_config  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/validate_run_config.py#L10)

Function to validate a provided run config blob against a given job.

If validation is successful, this function will return a dictionary representation of the
validated config actually used during execution.

Parameters:

- **job\_def** ( [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition)) – The job definition to validate run config against
- **run\_config** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The run config to validate

Returns: A dictionary representation of the validated config.Return type: Dict\[str, Any\]

### Run Config Schema [​](https://docs.dagster.io/api/dagster/execution\#run-config-schema "Direct link to Run Config Schema")

The `run_config` used for jobs has the following schema:

```codeBlockLines_e6Vv
{
  # configuration for execution, required if executors require config
  execution: {
    # the name of one, and only one available executor, typically 'in_process' or 'multiprocess'
    __executor_name__: {
      # executor-specific config, if required or permitted
      config: {
        ...
      }
    }
  },

  # configuration for loggers, required if loggers require config
  loggers: {
    # the name of an available logger
    __logger_name__: {
      # logger-specific config, if required or permitted
      config: {
        ...
      }
    },
    ...
  },

  # configuration for resources, required if resources require config
  resources: {
    # the name of a resource
    __resource_name__: {
      # resource-specific config, if required or permitted
      config: {
        ...
      }
    },
    ...
  },

  # configuration for underlying ops, required if ops require config
  ops: {

    # these keys align with the names of the ops, or their alias in this job
    __op_name__: {

      # pass any data that was defined via config_field
      config: ...,

      # configurably specify input values, keyed by input name
      inputs: {
        __input_name__: {
          # if an dagster_type_loader is specified, that schema must be satisfied here;
          # scalar, built-in types will generally allow their values to be specified directly:
          value: ...
        }
      },

    }
  },

}

```

- [Materializing Assets](https://docs.dagster.io/api/dagster/execution#materializing-assets)
- [Executing Jobs](https://docs.dagster.io/api/dagster/execution#executing-jobs)
- [Executing Graphs](https://docs.dagster.io/api/dagster/execution#executing-graphs)
- [Execution results](https://docs.dagster.io/api/dagster/execution#execution-results)
- [Reconstructable jobs](https://docs.dagster.io/api/dagster/execution#reconstructable-jobs)
- [Executors](https://docs.dagster.io/api/dagster/execution#executors)
- [Contexts](https://docs.dagster.io/api/dagster/execution#contexts)
- [Job configuration](https://docs.dagster.io/api/dagster/execution#job-configuration)
  - [Run Config Schema](https://docs.dagster.io/api/dagster/execution#run-config-schema)
