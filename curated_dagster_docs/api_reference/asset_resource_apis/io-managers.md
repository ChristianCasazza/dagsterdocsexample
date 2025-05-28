
On this page

IO managers are user-provided objects that store op outputs and load them as inputs to downstream
ops.

`class` dagster.ConfigurableIOManager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/io_manager.py#L202)

Base class for Dagster IO managers that utilize structured config.

This class is a subclass of both [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition), [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config),
and [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager). Implementers must provide an implementation of the
`handle_output()` and `load_input()` methods.

Example definition:

```codeBlockLines_e6Vv
class MyIOManager(ConfigurableIOManager):
    path_prefix: List[str]

    def _get_path(self, context) -> str:
        return "/".join(context.asset_key.path)

    def handle_output(self, context, obj):
        write_csv(self._get_path(context), obj)

    def load_input(self, context):
        return read_csv(self._get_path(context))

defs = Definitions(
    ...,
    resources={
        "io_manager": MyIOManager(path_prefix=["my", "prefix"])
    }
)

```

`class` dagster.ConfigurableIOManagerFactory  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/io_manager.py#L84)

Base class for Dagster IO managers that utilize structured config. This base class
is useful for cases in which the returned IO manager is not the same as the class itself
(e.g. when it is a wrapper around the actual IO manager implementation).

This class is a subclass of both [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) and [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config).
Implementers should provide an implementation of the `resource_function()` method,
which should return an instance of [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).

Example definition:

```codeBlockLines_e6Vv
class ExternalIOManager(IOManager):

    def __init__(self, connection):
        self._connection = connection

    def handle_output(self, context, obj):
        ...

    def load_input(self, context):
        ...

class ConfigurableExternalIOManager(ConfigurableIOManagerFactory):
    username: str
    password: str

    def create_io_manager(self, context) -> IOManager:
        with database.connect(username, password) as connection:
            return MyExternalIOManager(connection)

defs = Definitions(
    ...,
    resources={
        "io_manager": ConfigurableExternalIOManager(
            username="dagster",
            password=EnvVar("DB_PASSWORD")
        )
    }
)

```

`class` dagster.IOManager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L133)

Base class for user-provided IO managers.

IOManagers are used to store op outputs and load them as inputs to downstream ops.

Extend this class to handle how objects are loaded and stored. Users should implement
`handle_output` to store an object and `load_input` to retrieve an object.

`abstractmethod` handle\_output  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L155)

User-defined method that stores an output of an op.

Parameters:

- **context** ( [_OutputContext_](https://docs.dagster.io/api/dagster/io-managers#dagster.OutputContext)) – The context of the step output that produces this object.
- **obj** ( _Any_) – The object, returned by the op, to be stored.

`abstractmethod` load\_input  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L142)

User-defined method that loads an input to an op.

Parameters: **context** ( [_InputContext_](https://docs.dagster.io/api/dagster/io-managers#dagster.InputContext)) – The input context, which describes the input that’s being loaded
and the upstream output that’s being loaded from.Returns: The data object.Return type: Any

`class` dagster.IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L48)

Definition of an IO manager resource.

IOManagers are used to store op outputs and load them as inputs to downstream ops.

An IOManagerDefinition is a [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) whose resource\_fn returns an
[`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).

The easiest way to create an IOManagerDefnition is with the [`@io_manager`](https://docs.dagster.io/api/dagster/io-managers#dagster.io_manager)
decorator.

`static` hardcoded\_io\_manager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L115)

A helper function that creates an `IOManagerDefinition` with a hardcoded IOManager.

Parameters:

- **value** ( [_IOManager_](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager)) – A hardcoded IO Manager which helps mock the definition.
- **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the IO Manager. Defaults to None.

Returns: A hardcoded resource.Return type: \[ [IOManagerDefinition](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition)\]

@dagster.io\_manager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/io_manager.py#L181)

Define an IO manager.

IOManagers are used to store op outputs and load them as inputs to downstream ops.

The decorated function should accept an [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) and return an
[`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).

Parameters:

- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for the resource config. Configuration data available in init\_context.resource\_config. If not set, Dagster will accept any config provided.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.
- **output\_config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for per-output config. If not set, no per-output configuration will be allowed.
- **input\_config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for per-input config. If not set, Dagster will accept any config provided.
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Keys for the resources required by the object manager.
- **version** ( _Optional_ _\[_ _str_ _\]_) – The version of a resource function. Two wrapped resource functions should only have the same version if they produce the same resource definition when provided with the same inputs.

**Examples:**

```codeBlockLines_e6Vv
class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        write_csv("some/path")

    def load_input(self, context):
        return read_csv("some/path")

@io_manager
def my_io_manager(init_context):
    return MyIOManager()

@op(out=Out(io_manager_key="my_io_manager_key"))
def my_op(_):
    return do_stuff()

@job(resource_defs={"my_io_manager_key": my_io_manager})
def my_job():
    my_op()

```

## Input and Output Contexts [​](https://docs.dagster.io/api/dagster/io-managers\#input-and-output-contexts "Direct link to Input and Output Contexts")

`class` dagster.InputContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/input.py#L27)

The `context` object available to the load\_input method of [`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager).

Users should not instantiate this object directly. In order to construct
an InputContext for testing an IO Manager’s load\_input method, use
[`dagster.build_input_context()`](https://docs.dagster.io/api/dagster/io-managers#dagster.build_input_context).

Example:

```codeBlockLines_e6Vv
from dagster import IOManager, InputContext

class MyIOManager(IOManager):
    def load_input(self, context: InputContext):
        ...

```

get\_asset\_identifier  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/input.py#L443)

The sequence of strings making up the AssetKey for the asset being loaded as an input.
If the asset is partitioned, the identifier contains the partition key as the final element in the
sequence. For example, for the asset key `AssetKey(["foo", "bar", "baz"])`, materialized with
partition key “2023-06-01”, `get_asset_identifier` will return `["foo", "bar", "baz", "2023-06-01"]`.

get\_identifier  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/input.py#L416)

Utility method to get a collection of identifiers that as a whole represent a unique
step input.

If not using memoization, the unique identifier collection consists of

- `run_id`: the id of the run which generates the input.
- `step_key`: the key for a compute step.
- `name`: the name of the output. (default: ‘result’).

If using memoization, the `version` corresponding to the step output is used in place of
the `run_id`.

Returns: A list of identifiers, i.e. (run\_id or version), step\_key, and output\_nameReturn type: List\[str, …\]

`property` asset\_key

The `AssetKey` of the asset that is being loaded as an input.

`property` asset\_partition\_key

The partition key for input asset.

Raises an error if the input asset has no partitioning, or if the run covers a partition
range for the input asset.

`property` asset\_partition\_key\_range

The partition key range for input asset.

Raises an error if the input asset has no partitioning.

`property` asset\_partition\_keys

The partition keys for input asset.

Raises an error if the input asset has no partitioning.

`property` asset\_partitions\_def

The PartitionsDefinition on the upstream asset corresponding to this input.

`property` asset\_partitions\_time\_window

The time window for the partitions of the input asset.

Raises an error if either of the following are true:

- The input asset has no partitioning.
- The input asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

`property` config

The config attached to the input that we’re loading.

`property` dagster\_type

The type of this input.
Dagster types do not propagate from an upstream output to downstream inputs,
and this property only captures type information for the input that is either
passed in explicitly with [`AssetIn`](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn) or [`In`](https://docs.dagster.io/api/dagster/ops#dagster.In), or can be
infered from type hints. For an asset input, the Dagster type from the upstream
asset definition is ignored.

`property` definition\_metadata

A dict of metadata that is assigned to the InputDefinition that we’re loading.
This property only contains metadata passed in explicitly with [`AssetIn`](https://docs.dagster.io/api/dagster/assets#dagster.AssetIn)
or [`In`](https://docs.dagster.io/api/dagster/ops#dagster.In). To access metadata of an upstream asset or op definition,
use the definition\_metadata in [`InputContext.upstream_output`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputContext.upstream_output).

`property` has\_asset\_key

Returns True if an asset is being loaded as input, otherwise returns False. A return value of False
indicates that an output from an op is being loaded as the input.

`property` has\_asset\_partitions

Returns True if the asset being loaded as input is partitioned.

`property` has\_input\_name

If we’re the InputContext is being used to load the result of a run from outside the run,
then it won’t have an input name.

`property` has\_partition\_key

Whether the current run is a partitioned run.

`property` log

The log manager to use for this input.

`property` metadata

deprecated

This API will be removed in version 2.0.0.
Use definition\_metadata instead.

Use definitiion\_metadata instead.

Type: Deprecated

`property` name

The name of the input that we’re loading.

`property` op\_def

The definition of the op that’s loading the input.

`property` partition\_key

The partition key for the current run.

Raises an error if the current run is not a partitioned run.

`property` resource\_config

The config associated with the resource that initializes the InputManager.

`property` resources

The resources required by the resource that initializes the
input manager. If using the `@input_manager()` decorator, these resources
correspond to those requested with the required\_resource\_keys parameter.

`property` upstream\_output

Info about the output that produced the object we’re loading.

`class` dagster.OutputContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L40)

The context object that is available to the handle\_output method of an [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).

Users should not instantiate this object directly. To construct an
OutputContext for testing an IO Manager’s handle\_output method, use
[`dagster.build_output_context()`](https://docs.dagster.io/api/dagster/io-managers#dagster.build_output_context).

Example:

```codeBlockLines_e6Vv
from dagster import IOManager, OutputContext

class MyIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj):
        ...

```

add\_output\_metadata  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L691)

Add a dictionary of metadata to the handled output.

Metadata entries added will show up in the HANDLED\_OUTPUT and ASSET\_MATERIALIZATION events for the run.

Parameters: **metadata** ( _Mapping_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_) – A metadata dictionary to log
Examples:

```codeBlockLines_e6Vv
from dagster import IOManager

class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        context.add_output_metadata({"foo": "bar"})

```

get\_asset\_identifier  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L600)

The sequence of strings making up the AssetKey for the asset being stored as an output.
If the asset is partitioned, the identifier contains the partition key as the final element in the
sequence. For example, for the asset key `AssetKey(["foo", "bar", "baz"])` materialized with
partition key “2023-06-01”, `get_asset_identifier` will return `["foo", "bar", "baz", "2023-06-01"]`.

get\_identifier  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L554)

Utility method to get a collection of identifiers that as a whole represent a unique
step output.

If not using memoization, the unique identifier collection consists of

- `run_id`: the id of the run which generates the output.
- `step_key`: the key for a compute step.
- `name`: the name of the output. (default: ‘result’).

If using memoization, the `version` corresponding to the step output is used in place of
the `run_id`.

Returns: A list of identifiers, i.e. (run\_id or version), step\_key, and output\_nameReturn type: Sequence\[str, …\]

log\_event  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L623)

Log an AssetMaterialization or AssetObservation from within the body of an io manager’s handle\_output method.

Events logged with this method will appear in the event log.

Parameters: **event** ( _Union_ _\[_ [_AssetMaterialization_](https://docs.dagster.io/api/dagster/ops#dagster.AssetMaterialization) _,_ [_AssetObservation_](https://docs.dagster.io/api/dagster/assets#dagster.AssetObservation) _\]_) – The event to log.
Examples:

```codeBlockLines_e6Vv
from dagster import IOManager, AssetMaterialization

class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        context.log_event(AssetMaterialization("foo"))

```

`property` asset\_key

The `AssetKey` of the asset that is being stored as an output.

`property` asset\_partition\_key

The partition key for output asset.

Raises an error if the output asset has no partitioning, or if the run covers a partition
range for the output asset.

`property` asset\_partition\_key\_range

The partition key range for output asset.

Raises an error if the output asset has no partitioning.

`property` asset\_partition\_keys

The partition keys for the output asset.

Raises an error if the output asset has no partitioning.

`property` asset\_partitions\_def

The PartitionsDefinition on the asset corresponding to this output.

`property` asset\_partitions\_time\_window

The time window for the partitions of the output asset.

Raises an error if either of the following are true:

- The output asset has no partitioning.
- The output asset is not partitioned with a TimeWindowPartitionsDefinition or a
MultiPartitionsDefinition with one time-partitioned dimension.

`property` asset\_spec

The `AssetSpec` that is being stored as an output.

`property` config

The configuration for the output.

`property` dagster\_type

The type of this output.

`property` definition\_metadata

A dict of the metadata that is assigned to the OutputDefinition that produced
the output. Metadata is assigned to an OutputDefinition either directly on the OutputDefinition
or in the @asset decorator.

`property` has\_asset\_key

Returns True if an asset is being stored, otherwise returns False. A return value of False
indicates that an output from an op is being stored.

`property` has\_asset\_partitions

Returns True if the asset being stored is partitioned.

`property` has\_partition\_key

Whether the current run is a partitioned run.

`property` log

The log manager to use for this output.

`property` mapping\_key

The key that identifies a unique mapped output. None for regular outputs.

`property` metadata

deprecated

This API will be removed in version 2.0.0.
Use definition\_metadata instead.

used definition\_metadata instead.

Type: Deprecated

`property` name

The name of the output that produced the output.

`property` op\_def

The definition of the op that produced the output.

`property` output\_metadata

A dict of the metadata that is assigned to the output at execution time.

`property` partition\_key

The partition key for the current run.

Raises an error if the current run is not a partitioned run.

`property` resource\_config

The config associated with the resource that initializes the InputManager.

`property` resources

The resources required by the output manager, specified by the required\_resource\_keys
parameter.

`property` run\_id

The id of the run that produced the output.

`property` step\_key

The step\_key for the compute step that produced the output.

`property` version

The version of the output.

dagster.build\_input\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/input.py#L538)

Builds input context from provided parameters.

`build_input_context` can be used as either a function, or a context manager. If resources
that are also context managers are provided, then `build_input_context` must be used as a
context manager.

Parameters:

- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the input that we’re loading.
- **config** ( _Optional_ _\[_ _Any_ _\]_) – The config attached to the input that we’re loading.
- **definition\_metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of metadata that is assigned to the InputDefinition that we’re loading for.
- **upstream\_output** ( _Optional_ _\[_ [_OutputContext_](https://docs.dagster.io/api/dagster/io-managers#dagster.OutputContext) _\]_) – Info about the output that produced the object we’re loading.
- **dagster\_type** ( _Optional_ _\[_ [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType) _\]_) – The type of this input.
- **resource\_config** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resource config to make available from the input context. This usually corresponds to the config provided to the resource that loads the input manager.
- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources to make available from the context. For a given key, you can provide either an actual instance of an object, or a resource definition.
- **asset\_key** ( _Optional_ _\[_ _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _str_ _\]_ _\]_) – The asset key attached to the InputDefinition.
- **op\_def** ( _Optional_ _\[_ [_OpDefinition_](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition) _\]_) – The definition of the op that’s loading the input.
- **step\_context** ( _Optional_ _\[_ [_StepExecutionContext_](https://docs.dagster.io/api/dagster/internals#dagster.StepExecutionContext) _\]_) – For internal use.
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – String value representing partition key to execute with.
- **asset\_partition\_key\_range** ( _Optional_ _\[_ [_PartitionKeyRange_](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionKeyRange) _\]_) – The range of asset partition keys to load.
- **asset\_partitions\_def** – Optional\[PartitionsDefinition\]: The PartitionsDefinition of the asset being loaded.

Examples:

```codeBlockLines_e6Vv
build_input_context()

with build_input_context(resources={"foo": context_manager_resource}) as context:
    do_something

```

dagster.build\_output\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L815)

Builds output context from provided parameters.

`build_output_context` can be used as either a function, or a context manager. If resources
that are also context managers are provided, then `build_output_context` must be used as a
context manager.

Parameters:

- **step\_key** ( _Optional_ _\[_ _str_ _\]_) – The step\_key for the compute step that produced the output.
- **name** ( _Optional_ _\[_ _str_ _\]_) – The name of the output that produced the output.
- **definition\_metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict of the metadata that is assigned to the OutputDefinition that produced the output.
- **mapping\_key** ( _Optional_ _\[_ _str_ _\]_) – The key that identifies a unique mapped output. None for regular outputs.
- **config** ( _Optional_ _\[_ _Any_ _\]_) – The configuration for the output.
- **dagster\_type** ( _Optional_ _\[_ [_DagsterType_](https://docs.dagster.io/api/dagster/types#dagster.DagsterType) _\]_) – The type of this output.
- **version** ( _Optional_ _\[_ _str_ _\]_) – The version of the output.
- **resource\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resource config to make available from the input context. This usually corresponds to the config provided to the resource that loads the output manager.
- **resources** ( _Optional_ _\[_ _Resources_ _\]_) – The resources to make available from the context. For a given key, you can provide either an actual instance of an object, or a resource definition.
- **op\_def** ( _Optional_ _\[_ [_OpDefinition_](https://docs.dagster.io/api/dagster/ops#dagster.OpDefinition) _\]_) – The definition of the op that produced the output.
- **asset\_key** – Optional\[Union\[AssetKey, Sequence\[str\], str\]\]: The asset key corresponding to the output.
- **partition\_key** – Optional\[str\]: String value representing partition key to execute with.
- **metadata** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – deprecated Deprecated. Use definition\_metadata instead.

Examples:

```codeBlockLines_e6Vv
build_output_context()

with build_output_context(resources={"foo": context_manager_resource}) as context:
    do_something

```

## Built-in IO Managers [​](https://docs.dagster.io/api/dagster/io-managers\#built-in-io-managers "Direct link to Built-in IO Managers")

dagster.FilesystemIOManager IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/fs_io_manager.py#L29)

Built-in filesystem IO manager that stores and retrieves values using pickling.

The base directory that the pickle files live inside is determined by:

- The IO manager’s “base\_dir” configuration value, if specified. Otherwise…
- A “storage/” directory underneath the value for “local\_artifact\_storage” in your dagster.yaml file, if specified. Otherwise…
- A “storage/” directory underneath the directory that the DAGSTER\_HOME environment variable points to, if that environment variable is specified. Otherwise…
- A temporary directory.

Assigns each op output to a unique filepath containing run ID, step key, and output name.
Assigns each asset to a single filesystem path, at “<base\_dir>/<asset\_key>”. If the asset key
has multiple components, the final component is used as the name of the file, and the preceding
components as parent directories under the base\_dir.

Subsequent materializations of an asset will overwrite previous materializations of that asset.
So, with a base directory of “/my/base/path”, an asset with key
AssetKey(\[“one”, “two”, “three”\]) would be stored in a file called “three” in a directory
with path “/my/base/path/one/two/”.

Example usage:

1. Attach an IO manager to a set of assets using the reserved resource key `"io_manager"`.





```codeBlockLines_e6Vv
from dagster import Definitions, asset, FilesystemIOManager

@asset
def asset1():
       # create df ...
       return df

@asset
def asset2(asset1):
       return asset1[:5]

defs = Definitions(
       assets=[asset1, asset2],
       resources={
           "io_manager": FilesystemIOManager(base_dir="/my/base/path")
       },
)

```

2. Specify a job-level IO manager using the reserved resource key `"io_manager"`,
which will set the given IO manager on all ops in a job.





```codeBlockLines_e6Vv
from dagster import FilesystemIOManager, job, op

@op
def op_a():
       # create df ...
       return df

@op
def op_b(df):
       return df[:5]

@job(
       resource_defs={
           "io_manager": FilesystemIOManager(base_dir="/my/base/path")
       }
)
def job():
       op_b(op_a())

```

3. Specify IO manager on [`Out`](https://docs.dagster.io/api/dagster/ops#dagster.Out), which allows you to set different IO managers on
different step outputs.





```codeBlockLines_e6Vv
from dagster import FilesystemIOManager, job, op, Out

@op(out=Out(io_manager_key="my_io_manager"))
def op_a():
       # create df ...
       return df

@op
def op_b(df):
       return df[:5]

@job(resource_defs={"my_io_manager": FilesystemIOManager()})
def job():
       op_b(op_a())

```


dagster.InMemoryIOManager IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/mem_io_manager.py#L6)

I/O manager that stores and retrieves values in memory. After execution is complete, the values will
be garbage-collected. Note that this means that each run will not have access to values from previous runs.

The `UPathIOManager` can be used to easily define filesystem-based IO Managers.

`class` dagster.UPathIOManager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/upath_io_manager.py#L22)

Abstract IOManager base class compatible with local and cloud storage via universal-pathlib and fsspec.

Features:

- handles partitioned assets
- handles loading a single upstream partition
- handles loading multiple upstream partitions (with respect to [`PartitionMapping`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionMapping))
- supports loading multiple partitions concurrently with async load\_from\_path method
- the get\_metadata method can be customized to add additional metadata to the output
- the allow\_missing\_partitions metadata value can be set to True to skip missing partitions (the default behavior is to raise an error)

## Input Managers [​](https://docs.dagster.io/api/dagster/io-managers\#input-managers "Direct link to Input Managers")

Input managers load inputs from either upstream outputs or from provided default values.

@dagster.input\_manager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/input_manager.py#L124)

Define an input manager.

Input managers load op inputs, either from upstream outputs or by providing default values.

The decorated function should accept a [`InputContext`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputContext) and resource config, and return
a loaded object that will be passed into one of the inputs of an op.

The decorator produces an [`InputManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManagerDefinition).

Parameters:

- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for the resource-level config. If not set, Dagster will accept any config provided.
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.
- **input\_config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – A schema for the input-level config. Each input that uses this input manager can be configured separately using this config. If not set, Dagster will accept any config provided.
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Keys for the resources required by the input manager.
- **version** ( _Optional_ _\[_ _str_ _\]_) – The version of the input manager definition.

**Examples:**

```codeBlockLines_e6Vv
from dagster import input_manager, op, job, In

@input_manager
def csv_loader(_):
    return read_csv("some/path")

@op(ins={"input1": In(input_manager_key="csv_loader_key")})
def my_op(_, input1):
    do_stuff(input1)

@job(resource_defs={"csv_loader_key": csv_loader})
def my_job():
    my_op()

@input_manager(config_schema={"base_dir": str})
def csv_loader(context):
    return read_csv(context.resource_config["base_dir"] + "/some/path")

@input_manager(input_config_schema={"path": str})
def csv_loader(context):
    return read_csv(context.config["path"])

```

`class` dagster.InputManager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/input_manager.py#L34)

Base interface for classes that are responsible for loading solid inputs.

`class` dagster.InputManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/input_manager.py#L58)

Definition of an input manager resource.

Input managers load op inputs.

An InputManagerDefinition is a [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) whose resource\_fn returns an
[`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager).

The easiest way to create an InputManagerDefinition is with the
[`@input_manager`](https://docs.dagster.io/api/dagster/io-managers#dagster.input_manager) decorator.

## Legacy [​](https://docs.dagster.io/api/dagster/io-managers\#legacy "Direct link to Legacy")

dagster.fs\_io\_manager IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/fs_io_manager.py#L135)

superseded

This API has been superseded.
Use FilesystemIOManager directly instead.

Built-in filesystem IO manager that stores and retrieves values using pickling.

The base directory that the pickle files live inside is determined by:

- The IO manager’s “base\_dir” configuration value, if specified. Otherwise…
- A “storage/” directory underneath the value for “local\_artifact\_storage” in your dagster.yaml file, if specified. Otherwise…
- A “storage/” directory underneath the directory that the DAGSTER\_HOME environment variable points to, if that environment variable is specified. Otherwise…
- A temporary directory.

Assigns each op output to a unique filepath containing run ID, step key, and output name.
Assigns each asset to a single filesystem path, at “<base\_dir>/<asset\_key>”. If the asset key
has multiple components, the final component is used as the name of the file, and the preceding
components as parent directories under the base\_dir.

Subsequent materializations of an asset will overwrite previous materializations of that asset.
So, with a base directory of “/my/base/path”, an asset with key
AssetKey(\[“one”, “two”, “three”\]) would be stored in a file called “three” in a directory
with path “/my/base/path/one/two/”.

Example usage:

1. Attach an IO manager to a set of assets using the reserved resource key `"io_manager"`.





```codeBlockLines_e6Vv
from dagster import Definitions, asset, fs_io_manager

@asset
def asset1():
       # create df ...
       return df

@asset
def asset2(asset1):
       return asset1[:5]

defs = Definitions(
       assets=[asset1, asset2],
       resources={
           "io_manager": fs_io_manager.configured({"base_dir": "/my/base/path"})
       },
)

```

2. Specify a job-level IO manager using the reserved resource key `"io_manager"`,
which will set the given IO manager on all ops in a job.





```codeBlockLines_e6Vv
from dagster import fs_io_manager, job, op

@op
def op_a():
       # create df ...
       return df

@op
def op_b(df):
       return df[:5]

@job(
       resource_defs={
           "io_manager": fs_io_manager.configured({"base_dir": "/my/base/path"})
       }
)
def job():
       op_b(op_a())

```

3. Specify IO manager on [`Out`](https://docs.dagster.io/api/dagster/ops#dagster.Out), which allows you to set different IO managers on
different step outputs.





```codeBlockLines_e6Vv
from dagster import fs_io_manager, job, op, Out

@op(out=Out(io_manager_key="my_io_manager"))
def op_a():
       # create df ...
       return df

@op
def op_b(df):
       return df[:5]

@job(resource_defs={"my_io_manager": fs_io_manager})
def job():
       op_b(op_a())

```


dagster.mem\_io\_manager IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/mem_io_manager.py#L23)

Built-in IO manager that stores and retrieves values in memory.

- [Input and Output Contexts](https://docs.dagster.io/api/dagster/io-managers#input-and-output-contexts)
- [Built-in IO Managers](https://docs.dagster.io/api/dagster/io-managers#built-in-io-managers)
- [Input Managers](https://docs.dagster.io/api/dagster/io-managers#input-managers)
- [Legacy](https://docs.dagster.io/api/dagster/io-managers#legacy)
