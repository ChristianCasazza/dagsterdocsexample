Now, build on our base knowledge of Dagster. We can use this to more deeply understand how to use Dagster in a more advanced way. First, explain the new information I am providing you in a detailed and comprehensive way. Then, connect it back to our base Dagster knowledge, and provide a comprehensive overview of our understanding of Dagster. Then, expand our example to incorporate our new information

<docs>
# Repository Content

## Directory Structure (Selected Files)

├── curated_dagster_docs/foundation/api_quick_index/io-managers.md
├── curated_dagster_docs/foundation/orchestration_automation/api_orchestration/jobs.md
├── curated_dagster_docs/foundation/orchestration_automation/api_orchestration/schedules-sensors.md
├── curated_dagster_docs/foundation/orchestration_automation/asset_jobs_selection/asset-jobs.md
├── curated_dagster_docs/foundation/orchestration_automation/asset_jobs_selection/asset-selection-syntax.md
├── curated_dagster_docs/foundation/orchestration_automation/declarative_automation/declarative-automation.md
├── curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/job-execution.md
├── curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/op-jobs.md
├── curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/unconnected-inputs.md
├── curated_dagster_docs/foundation/orchestration_automation/schedules_time/defining-schedules.md
├── curated_dagster_docs/foundation/orchestration_automation/schedules_time/partitioning-assets.md
├── curated_dagster_docs/foundation/orchestration_automation/sensors_event/asset-sensors.md
├── curated_dagster_docs/foundation/orchestration_automation/sensors_event/run-status-sensors.md
├── curated_dagster_docs/foundation/orchestration_automation/sensors_event/sensors.md

## Files

### File: curated_dagster_docs/foundation/api_quick_index/io-managers.md
markdown

On this page

IO managers are user-provided objects that store op outputs and load them as inputs to downstream
ops.

`class` dagster.ConfigurableIOManager  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/io_manager.py#L202)

Base class for Dagster IO managers that utilize structured config.

This class is a subclass of both [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition), [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config),
and [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager). Implementers must provide an implementation of the
`handle_output()` and `load_input()` methods.

Example definition:
codeBlockLines_e6Vv
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

`class` dagster.ConfigurableIOManagerFactory  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/io_manager.py#L84)

Base class for Dagster IO managers that utilize structured config. This base class
is useful for cases in which the returned IO manager is not the same as the class itself
(e.g. when it is a wrapper around the actual IO manager implementation).

This class is a subclass of both [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition) and [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config).
Implementers should provide an implementation of the `resource_function()` method,
which should return an instance of [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager).

Example definition:
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
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

## Input and Output Contexts [​](https://docs.dagster.io/api/dagster/io-managers\#input-and-output-contexts "Direct link to Input and Output Contexts")

`class` dagster.InputContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/input.py#L27)

The `context` object available to the load\_input method of [`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager).

Users should not instantiate this object directly. In order to construct
an InputContext for testing an IO Manager’s load\_input method, use
[`dagster.build_input_context()`](https://docs.dagster.io/api/dagster/io-managers#dagster.build_input_context).

Example:
codeBlockLines_e6Vv
from dagster import IOManager, InputContext

class MyIOManager(IOManager):
    def load_input(self, context: InputContext):
        ...

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
codeBlockLines_e6Vv
from dagster import IOManager, OutputContext

class MyIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj):
        ...

add\_output\_metadata  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/output.py#L691)

Add a dictionary of metadata to the handled output.

Metadata entries added will show up in the HANDLED\_OUTPUT and ASSET\_MATERIALIZATION events for the run.

Parameters: **metadata** ( _Mapping_ _\[_ _str_ _,_ _RawMetadataValue_ _\]_) – A metadata dictionary to log
Examples:
codeBlockLines_e6Vv
from dagster import IOManager

class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        context.add_output_metadata({"foo": "bar"})

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
codeBlockLines_e6Vv
from dagster import IOManager, AssetMaterialization

class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        context.log_event(AssetMaterialization("foo"))

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
codeBlockLines_e6Vv
build_input_context()

with build_input_context(resources={"foo": context_manager_resource}) as context:
    do_something

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
codeBlockLines_e6Vv
build_output_context()

with build_output_context(resources={"foo": context_manager_resource}) as context:
    do_something

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
codeBlockLines_e6Vv
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

2. Specify a job-level IO manager using the reserved resource key `"io_manager"`,
which will set the given IO manager on all ops in a job.
codeBlockLines_e6Vv
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

3. Specify IO manager on [`Out`](https://docs.dagster.io/api/dagster/ops#dagster.Out), which allows you to set different IO managers on
different step outputs.
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
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

2. Specify a job-level IO manager using the reserved resource key `"io_manager"`,
which will set the given IO manager on all ops in a job.
codeBlockLines_e6Vv
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

3. Specify IO manager on [`Out`](https://docs.dagster.io/api/dagster/ops#dagster.Out), which allows you to set different IO managers on
different step outputs.
codeBlockLines_e6Vv
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

dagster.mem\_io\_manager IOManagerDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/storage/mem_io_manager.py#L23)

Built-in IO manager that stores and retrieves values in memory.

- [Input and Output Contexts](https://docs.dagster.io/api/dagster/io-managers#input-and-output-contexts)
- [Built-in IO Managers](https://docs.dagster.io/api/dagster/io-managers#built-in-io-managers)
- [Input Managers](https://docs.dagster.io/api/dagster/io-managers#input-managers)
- [Legacy](https://docs.dagster.io/api/dagster/io-managers#legacy)


### File: curated_dagster_docs/foundation/orchestration_automation/api_orchestration/jobs.md
markdown

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
codeBlockLines_e6Vv
@op
def return_one():
    return 1

@op
def add_one(in1):
    return in1 + 1

@job
def job1():
    add_one(return_one())

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
codeBlockLines_e6Vv
from dagster import graph, reconstructable

@graph
def my_graph():
    ...

def define_my_job():
    return my_graph.to_job()

reconstructable(define_my_job)

This function implements a very conservative strategy for reconstruction, so that its behavior
is easy to predict, but as a consequence it is not able to reconstruct certain kinds of jobs
or jobs, such as those defined by lambdas, in nested scopes (e.g., dynamically within a method
call), or in interactive environments such as the Python REPL or Jupyter notebooks.

If you need to reconstruct objects constructed in these ways, you should use
`build_reconstructable_job()` instead, which allows you to
specify your own reconstruction strategy.

Examples:
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
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

- [Reconstructable jobs](https://docs.dagster.io/api/dagster/jobs#reconstructable-jobs)


### File: curated_dagster_docs/foundation/orchestration_automation/api_orchestration/schedules-sensors.md
markdown

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
codeBlockLines_e6Vv
from dagster import schedule, ScheduleEvaluationContext

@schedule
def the_schedule(context: ScheduleEvaluationContext):
    ...

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
codeBlockLines_e6Vv
context = build_schedule_context(instance)

dagster.build\_schedule\_from\_partitioned\_job  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/partitioned_schedule.py#L63)

Creates a schedule from a job that targets
time window-partitioned or statically-partitioned assets. The job can also be
multi-partitioned, as long as one of the partition dimensions is time-partitioned.

The schedule executes at the cadence specified by the time partitioning of the job or assets.

**Example:**
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
from dagster import sensor, SensorEvaluationContext

@sensor
def the_sensor(context: SensorEvaluationContext):
    ...

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
codeBlockLines_e6Vv
context = build_sensor_context()
my_sensor(context)

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
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
error_strings_by_step_key = {
    # includes the stack trace
    event.step_key: event.event_specific_data.error.to_string()
    for event in context.get_step_failure_events()
}

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
codeBlockLines_e6Vv
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


### File: curated_dagster_docs/foundation/orchestration_automation/asset_jobs_selection/asset-jobs.md
markdown

On this page

Jobs are the main unit of execution and monitoring for [asset definitions](https://docs.dagster.io/guides/build/assets/defining-assets) in Dagster. An asset job is a type of job that targets a [selection of assets](https://docs.dagster.io/guides/build/assets/asset-selection-syntax) and can be launched:

- Manually from the Dagster UI
- At fixed intervals, by [schedules](https://docs.dagster.io/guides/automate/schedules)
- When external changes occur, using [sensors](https://docs.dagster.io/guides/automate/sensors)

## Creating asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#creating-asset-jobs "Direct link to Creating asset jobs")

In this section, we'll demonstrate how to create a few asset jobs that target the following assets:
codeBlockLines_e6Vv
@dg.asset
def sugary_cereals() -> None:
    execute_query(
        "CREATE TABLE sugary_cereals AS SELECT * FROM cereals WHERE sugar_grams > 10"
    )

@dg.asset(deps=[sugary_cereals])
def shopping_list() -> None:
    execute_query("CREATE TABLE shopping_list AS SELECT * FROM sugary_cereals")

To create an asset job, use the [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) method. An asset-based job is based on the assets the job targets and their dependencies.

You can target one or multiple assets, or create multiple jobs that target overlapping sets of assets. In the following example, we have two jobs:

- `all_assets_job` targets all assets
- `sugary_cereals_job` targets only the `sugary_cereals` asset
codeBlockLines_e6Vv
all_assets_job = dg.define_asset_job(name="all_assets_job")

sugary_cereals_job = dg.define_asset_job(
    name="sugary_cereals_job", selection="sugary_cereals"
)

defs = dg.Definitions(
    assets=[sugary_cereals, shopping_list],
    jobs=[all_assets_job, sugary_cereals_job],
)

## Making asset jobs available to Dagster tools [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#making-asset-jobs-available-to-dagster-tools "Direct link to Making asset jobs available to Dagster tools")

Including the jobs in a [`Definitions`](https://docs.dagster.io/api/dagster/definitions) object located at the top level of a Python module or file makes asset jobs available to the UI, GraphQL, and the command line. The Dagster tool loads that module as a code location. If you include schedules or sensors, the [code location](https://docs.dagster.io/guides/deploy/code-locations) will automatically include jobs that those schedules or sensors target.
codeBlockLines_e6Vv
from dagster import Definitions, MaterializeResult, asset, define_asset_job

@asset
def number_asset():
    yield MaterializeResult(
        metadata={
            "number": 1,
        }
    )

number_asset_job = define_asset_job(name="number_asset_job", selection="number_asset")

defs = Definitions(
    assets=[number_asset],
    jobs=[number_asset_job],
)

## Testing asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#testing-asset-jobs "Direct link to Testing asset jobs")

Dagster has built-in support for testing, including separating business logic from environments and setting explicit expectations on uncontrollable inputs. For more information, see the [testing documentation](https://docs.dagster.io/guides/test).

## Executing asset jobs [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#executing-asset-jobs "Direct link to Executing asset jobs")

You can run an asset job in a variety of ways:

- In the Python process where it's defined
- Via the command line
- Via the GraphQL API
- In the UI

## Examples [​](https://docs.dagster.io/guides/build/jobs/asset-jobs\#examples "Direct link to Examples")

The [Hacker News example](https://github.com/dagster-io/dagster/tree/master/examples/project_fully_featured) [builds an asset job that targets an asset group](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/jobs.py).

- [Creating asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#creating-asset-jobs)
- [Making asset jobs available to Dagster tools](https://docs.dagster.io/guides/build/jobs/asset-jobs#making-asset-jobs-available-to-dagster-tools)
- [Testing asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#testing-asset-jobs)
- [Executing asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs#executing-asset-jobs)
- [Examples](https://docs.dagster.io/guides/build/jobs/asset-jobs#examples)


### File: curated_dagster_docs/foundation/orchestration_automation/asset_jobs_selection/asset-selection-syntax.md
markdown

On this page

Dagster's asset selection syntax allows you to query and view assets within your data lineage graph. You can select upstream and downstream layers of the graph, use filters to narrow down your selection, and use functions to return the root or sink assets of a given selection.

With asset selection, you can:

- Select a set of assets to view in the Dagster UI
- Define a job in Python that targets a selection of assets
- List or materialize a set of assets using the [Dagster CLI](https://docs.dagster.io/api/dagster/cli#dagster-asset)

## Availability [​](https://docs.dagster.io/guides/build/assets/asset-selection-syntax\#availability "Direct link to Availability")

In the **Dagster OSS UI**, the asset selection syntax is available on:

- The Asset Catalog
- The Global asset lineage page

In the **Dagster+ UI**, the asset selection syntax is available on:

- The Asset Catalog > All Assets page
- The Global asset lineage page
- The Asset Health page
- The Insights page
- The Alert Policy creation page (when creating an asset alert)

## Next steps [​](https://docs.dagster.io/guides/build/assets/asset-selection-syntax\#next-steps "Direct link to Next steps")

- See the [asset selection syntax reference](https://docs.dagster.io/guides/build/assets/asset-selection-syntax/reference) for a full list of the filters, layers, operands, and functions that you can use to construct your own queries.
- Check out [example asset selection queries](https://docs.dagster.io/guides/build/assets/asset-selection-syntax/examples).

- [Availability](https://docs.dagster.io/guides/build/assets/asset-selection-syntax#availability)
- [Next steps](https://docs.dagster.io/guides/build/assets/asset-selection-syntax#next-steps)


### File: curated_dagster_docs/foundation/orchestration_automation/declarative_automation/declarative-automation.md
markdown

On this page

Declarative Automation is a framework that uses information about the status of your assets and their dependencies to launch executions of your assets.

Not sure what automation method to use? Check out the [automation overview](https://docs.dagster.io/guides/automate) for a comparison of the different automation methods available in Dagster.

note

To use Declarative Automation, you will need to enable the default **[automation condition sensor](https://docs.dagster.io/guides/automate/declarative-automation/automation-condition-sensors)** in the UI, which evaluates automation conditions and launches runs in response to their statuses. To do so, navigate to **Automation**, find the desired code location, and toggle on the **default\_automation\_condition\_sensor** sensor.

## Recommended automation conditions [​](https://docs.dagster.io/guides/automate/declarative-automation\#recommended-automation-conditions "Direct link to Recommended automation conditions")

An [`AutomationCondition`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition) on an asset or asset check describe the conditions under which work should be executed.

While the system can be customized to fit your specific needs, we recommend starting with one of the built-in conditions below, rather than building up your own condition from scratch.

- on\_cron
- eager
- on\_missing

The [`AutomationCondition.on_cron`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.on_cron) will execute an asset once per cron schedule tick, after all upstream dependencies have updated. This allows you to schedule assets to execute on a regular cadence regardless of the exact time that upstream data arrives.

In the example below, the asset will start waiting for each of its dependencies to be updated at the start of each hour. Once all dependencies have updated since the start of the hour, this asset will be immediately requested.
codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    deps=["upstream"],
    automation_condition=dg.AutomationCondition.on_cron("@hourly"),
)
def hourly_asset() -> None: ...

**Behavior**

If you would like to customize aspects of this behavior, refer to the [customizing on\_cron](https://docs.dagster.io/guides/automate/customizing-automation-conditions/customizing-on-cron-condition) guide.

- If at least one upstream partition of _all_ upstream assets has been updated since the previous cron schedule tick, and the downstream asset has not yet been requested or updated, the downstream asset will be requested.
- If all upstream assets **do not** update within the given cron tick, the downstream asset will not be requested.
- For **time-partitioned** assets, this condition will only request the _latest_ time partition of the asset.
- For **static** and **dynamic-partitioned** assets, this condition will request _all_ partitions of the asset.

The [`AutomationCondition.eager`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.eager) condition allows you to automatically update an asset whenever any of its dependencies are updated. This is used to ensure that whenever upstream changes happen, they are automatically propagated downstream.

In the example below, the following asset will be automatically updated whenever any of its upstream dependencies are updated:
codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    deps=["upstream"],
    automation_condition=dg.AutomationCondition.eager(),
)
def eager_asset() -> None: ...

**Behavior**

- If _any_ upstream partitions have not been materialized, the downstream asset will not be requested.
- If _any_ upstream partitions are currently part of an in-progress run, the downstream asset will wait for those runs to complete before being requested.
- If the downstream asset is already part of an in-progress run, the downstream asset will wait for that run to complete before being requested.
- For **time-partitioned** assets, this condition will only consider the _latest_ time partition of the asset.
- For **static** and **dynamic-partitioned** assets, this condition will consider _all_ partitions of the asset.
- If an upstream asset is _observed_, this will only be treated as an update to the upstream asset if the data version has changed since the previous observation.
- If an upstream asset is _materialized_, this will be treated as an update to the upstream asset regardless of the data version of that materialization.

To customize this behavior, see the [customizing eager](https://docs.dagster.io/guides/automate/customizing-automation-conditions/customizing-eager-condition) guide.

The [`AutomationCondition.on_missing`](https://docs.dagster.io/api/dagster/assets#dagster.AutomationCondition.on_missing) condition will execute a missing asset partition when all upstream partitions of the asset are available. This is typically used to manage dependencies between partitioned assets, where partitions should fill in as soon as upstream data is available.

In the example below, as soon as all hourly partitions of the upstream asset are filled in, the downstream asset will be immediately requested:
codeBlockLines_e6Vv
import dagster as dg

@dg.asset(
    partitions_def=dg.HourlyPartitionsDefinition("2025-01-01-00:00"),
)
def upstream() -> None: ...

@dg.asset(
    deps=[upstream],
    automation_condition=dg.AutomationCondition.on_missing(),
    partitions_def=dg.DailyPartitionsDefinition("2025-01-01"),
)
def downstream() -> None: ...

**Behavior**

- If _any_ upstream partition of any upstream asset has not been materialized, the downstream asset will not be requested.
- For **time-partitioned** assets, this condition will only request the _latest_ time partition of the asset.
- This condition will only consider partitions that were added to the asset after the condition was enabled.

To customize this behavior, see the [customizing on\_missing](https://docs.dagster.io/guides/automate/customizing-automation-conditions/customizing-on-missing-condition) guide.

- [Recommended automation conditions](https://docs.dagster.io/guides/automate/declarative-automation#recommended-automation-conditions)


### File: curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/job-execution.md
markdown

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
codeBlockLines_e6Vv
dagster dev -f my_job.py

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
codeBlockLines_e6Vv
dagster job execute -f my_job.py

### Python APIs [​](https://docs.dagster.io/guides/build/jobs/job-execution\#python-apis "Direct link to Python APIs")

Dagster includes Python APIs for execution that are useful when writing tests or scripts.

[`JobDefinition.execute_in_process`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition.execute_in_process) executes a job and
returns an [`ExecuteInProcessResult`](https://docs.dagster.io/api/dagster/execution#dagster.ExecuteInProcessResult).
codeBlockLines_e6Vv
Loading...

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
codeBlockLines_e6Vv
    my_job.execute_in_process(op_selection=["*add_two"])

Similarly, you can specify the same op selection in the Dagster UI Launchpad:

![Op selection](https://docs.dagster.io/assets/images/solid-selection-6b1244d9173c7ef12cddcaba1084a2d4.png)

## Controlling job execution [​](https://docs.dagster.io/guides/build/jobs/job-execution\#controlling-job-execution "Direct link to Controlling job execution")

Each [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) contains an [`ExecutorDefinition`](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) that determines how it will be executed.

This `executor_def` property can be set to allow for different types of isolation and parallelism, ranging from executing all the ops in the same process to executing each op in its own Kubernetes pod. See [Executors](https://docs.dagster.io/guides/operate/run-executors) for more details.

### Default job executor [​](https://docs.dagster.io/guides/build/jobs/job-execution\#default-job-executor "Direct link to Default job executor")

The default job executor definition defaults to multiprocess execution. It also allows you to toggle between in-process and multiprocess execution via config.

Below is an example of run config as YAML you could provide in the Dagster UI playground to launch an in-process execution.
codeBlockLines_e6Vv

execution:
  config:
    in_process:

Additional config options are available for multiprocess execution that can help with performance. This includes limiting the max concurrent subprocesses and controlling how those subprocesses are spawned.

The example below sets the run config directly on the job to explicitly set the max concurrent subprocesses to `4`, and change the subprocess start method to use a forkserver.
codeBlockLines_e6Vv
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

Using a forkserver is a great way to reduce per-process overhead during multiprocess execution, but can cause issues with certain libraries. Refer to the [Python documentation](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) for more info.

#### Op concurrency limits [​](https://docs.dagster.io/guides/build/jobs/job-execution\#op-concurrency-limits "Direct link to Op concurrency limits")

In addition to the `max_concurrent` limit, you can use `tag_concurrency_limits` to specify limits on the number of ops with certain tags that can execute at once within a single run.

Limits can be specified for all ops with a certain tag key or key-value pair. If any limit would be exceeded by launching an op, then the op will stay queued. Asset jobs will look at the `op_tags` field on each asset in the job when checking them for tag concurrency limits.

For example, the following job will execute at most two ops at once with the `database` tag equal to `redshift`, while also ensuring that at most four ops execute at once:
codeBlockLines_e6Vv
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


### File: curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/op-jobs.md
markdown

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
codeBlockLines_e6Vv
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
codeBlockLines_e6Vv
from dagster import graph, op, ConfigurableResource

class Server(ConfigurableResource):
    def ping_server(self): ...

@op
def interact_with_server(server: Server):
    server.ping_server()

@graph
def do_stuff():
    interact_with_server()

Then build op jobs from it using the [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) method:
codeBlockLines_e6Vv
from dagster import ResourceDefinition

prod_server = ResourceDefinition.mock_resource()
local_server = ResourceDefinition.mock_resource()

prod_job = do_stuff.to_job(resource_defs={"server": prod_server}, name="do_stuff_prod")
local_job = do_stuff.to_job(
    resource_defs={"server": local_server}, name="do_stuff_local"
)

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
codeBlockLines_e6Vv
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

### Partitioned configuration [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#partitioned-configuration "Direct link to Partitioned configuration")

Supplying a [`PartitionedConfig`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionedConfig) will create a partitioned op job. This defines a discrete set of partitions along with a function for generating config for a partition. Op job runs can be configured by selecting a partition.

Refer to the [Partitioning ops documentation](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops) for more info and examples.

### Config mapping [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#config-mapping "Direct link to Config mapping")

Supplying a [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) allows you to expose a narrower config interface to the op job.

Instead of needing to configure every op and resource individually when launching the op job, you can supply a smaller number of values to the outer config. The [`ConfigMapping`](https://docs.dagster.io/api/dagster/config#dagster.ConfigMapping) will then translate it into config for all the job's ops and resources.
codeBlockLines_e6Vv
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

## Making op jobs available to Dagster tools [​](https://docs.dagster.io/guides/build/jobs/op-jobs\#making-op-jobs-available-to-dagster-tools "Direct link to Making op jobs available to Dagster tools")

You make jobs available to the UI, GraphQL, and command line by including them in a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object at the top level of Python module or file. The tool loads that module as a [code location](https://docs.dagster.io/guides/deploy/code-locations/). If you include schedules or sensors, the code location will automatically include jobs that those schedules or sensors target.
codeBlockLines_e6Vv
from dagster import Definitions, job

@job
def do_it_all(): ...

defs = Definitions(jobs=[do_it_all])

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


### File: curated_dagster_docs/foundation/orchestration_automation/jobs_deep_dive/unconnected-inputs.md
markdown

On this page

Assets vs ops

If you are just getting started with Dagster, we strongly recommend you use [assets](https://docs.dagster.io/guides/build/assets/) rather than ops to build your data pipelines. The ops documentation is for Dagster users who need to manage existing ops, or who have complex use cases.

Ops in a job may have input definitions that don't correspond to the outputs of upstream ops.

Values for these inputs can be provided in a few ways. Dagster will check the following, in order, and use the first available:

- **Input manager** \- If the input to a job comes from an external source, such as a table in a database, you may want to define a resource responsible for loading it. This makes it easy to swap out implementations in different jobs and mock it in tests.

A special I/O manager, which can be referenced from [`Ins`](https://docs.dagster.io/api/dagster/ops#dagster.In), can be used to load unconnected inputs. Refer to the [I/O manager documentation](https://docs.dagster.io/guides/build/io-managers) for more information about I/O managers.

- **Dagster Type loader** \- A [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) provides a way to specify how to load inputs that depends on a type. A [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) can be placed on [`DagsterType`](https://docs.dagster.io/api/dagster/types#dagster.DagsterType), which can be placed on [`In`](https://docs.dagster.io/api/dagster/ops#dagster.In).

- **Default values** \- [`In`](https://docs.dagster.io/api/dagster/ops#dagster.In) accepts a `default_value` argument.


**Unsure if I/O managers are right for you?** Check out the [Before you begin](https://docs.dagster.io/guides/build/io-managers/#before-you-begin) section of the I/O manager documentation.

## Working with Dagster types [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#working-with-dagster-types "Direct link to Working with Dagster types")

### Loading a built-in Dagster type from config [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#loading-a-built-in-dagster-type-from-config "Direct link to Loading a built-in Dagster type from config")

When you have an op at the beginning of a job that operates on a built-in Dagster type like `string` or `int`, you can provide a value for that input via run config.

Here's a basic job with an unconnected string input:
codeBlockLines_e6Vv
@op
def my_op(context: OpExecutionContext, input_string: str):
    context.log.info(f"input string: {input_string}")

@job
def my_job():
    my_op()

### Loading a custom Dagster type from config [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#loading-a-custom-dagster-type-from-config "Direct link to Loading a custom Dagster type from config")

When you have an op at the beginning of your job that operates on a Dagster type that you've defined, you can write your own [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) to define how to load that input via run config.
codeBlockLines_e6Vv
from typing import Union

import dagster as dg

@dg.dagster_type_loader(
    config_schema={"diameter": float, "juiciness": float, "cultivar": str}
)
def apple_loader(
    _context: dg.DagsterTypeLoaderContext, config: dict[str, Union[float, str]]
):
    return Apple(
        diameter=config["diameter"],
        juiciness=config["juiciness"],
        cultivar=config["cultivar"],
    )

@dg.usable_as_dagster_type(loader=apple_loader)
class Apple:
    def __init__(self, diameter, juiciness, cultivar):
        self.diameter = diameter
        self.juiciness = juiciness
        self.cultivar = cultivar

@dg.op
def my_op(context: dg.OpExecutionContext, input_apple: Apple):
    context.log.info(f"input apple diameter: {input_apple.diameter}")

@dg.job
def my_job():
    my_op()

With this, the input can be specified via config as below:
codeBlockLines_e6Vv
    my_job.execute_in_process(
        run_config={
            "ops": {
                "my_op": {
                    "inputs": {
                        "input_apple": {
                            "diameter": 2.4,
                            "juiciness": 6.0,
                            "cultivar": "honeycrisp",
                        }
                    }
                }
            }
        },
    )

## Working with input managers [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#working-with-input-managers "Direct link to Working with input managers")

### Providing an input manager for unconnected inputs [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#providing-an-input-manager-for-unconnected-inputs "Direct link to Providing an input manager for unconnected inputs")

When you have an op at the beginning of a job that operates on data from an external source, you might wish to separate that I/O from your op's business logic, in the same way you would with an I/O manager if the op were loading from an upstream output.

Use the following tabs to learn about how to achieve this in Dagster.

- Option 1: Use the input\_manager decorator
- Option 2: Use a class to implement the InputManager interface

In this example, we wrote a function to load the input and decorated it with [`@dg.input_manager`](https://docs.dagster.io/api/dagster/io-managers#dagster.input_manager):
codeBlockLines_e6Vv

@dg.input_manager
def simple_table_1_manager():
    return read_dataframe_from_table(name="table_1")

@dg.op(ins={"dataframe": dg.In(input_manager_key="simple_load_input_manager")})
def my_op(dataframe):
    """Do some stuff."""
    dataframe.head()

@dg.job(resource_defs={"simple_load_input_manager": simple_table_1_manager})
def simple_load_table_job():
    my_op()

In this example, we defined a class that implements the [`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager) interface:
codeBlockLines_e6Vv

class Table1InputManager(dg.InputManager):
    def load_input(self, context: dg.InputContext):
        return read_dataframe_from_table(name="table_1")

@dg.input_manager
def table_1_manager():
    return Table1InputManager()

@dg.job(resource_defs={"load_input_manager": table_1_manager})
def load_table_job():
    my_op()

To use `Table1InputManager` to store outputs or override the `load_input` method of an I/O manager used elsewhere in the job, another option is to implement an instance of [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager):
codeBlockLines_e6Vv
# in this example, TableIOManager is defined elsewhere and we just want to override load_input
class Table1IOManager(TableIOManager):
    def load_input(self, context: dg.InputContext):
        return read_dataframe_from_table(name="table_1")

@dg.job(resource_defs={"load_input_manager": Table1IOManager()})
def io_load_table_job():
    my_op()

In any of the examples in Option 1 or Option 2, setting the `input_manager_key` on an `In` controls how that input is loaded.

### Providing per-input config to input managers [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#providing-per-input-config-to-input-managers "Direct link to Providing per-input config to input managers")

When launching a run, you might want to parameterize how particular inputs are loaded.

To accomplish this, you can define an `input_config_schema` on the I/O manager or input manager definition. The `load_input` function can access this config when storing or loading data, via the [`InputContext`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputContext):
codeBlockLines_e6Vv

class MyConfigurableInputLoader(dg.InputManager):
    def load_input(self, context: dg.InputContext):
        return read_dataframe_from_table(name=context.config["table"])

@dg.input_manager(input_config_schema={"table": str})
def my_configurable_input_loader():
    return MyConfigurableInputLoader()

# or

@dg.input_manager(input_config_schema={"table": str})
def my_other_configurable_input_loader(context):
    return read_dataframe_from_table(name=context.config["table"])

Then, when executing a job, you can pass in this per-input config:
codeBlockLines_e6Vv

load_table_job.execute_in_process(
    run_config={"ops": {"my_op": {"inputs": {"dataframe": {"table": "table_1"}}}}},
)

### Using input managers with subselection [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#using-input-managers-with-subselection "Direct link to Using input managers with subselection")

You might want to execute a subset of ops in your job and control how the inputs of those ops are loaded. Custom input managers also help in these situations, because the inputs at the beginning of the subset become unconnected inputs.

For example, you might have `op1` that normally produces a table that `op2` consumes. To debug `op2`, you might want to run it on a different table than the one normally produced by `op1`.

To accomplish this, you can set up the `input_manager_key` on `op2`'s `In` to point to an input manager with the desired loading behavior. As in the previous example, setting the `input_manager_key` on an `In` controls how that input is loaded and you can write custom loading logic.
codeBlockLines_e6Vv

class MyIOManager(dg.ConfigurableIOManager):
    def handle_output(self, context: dg.OutputContext, obj):
        table_name = context.name
        write_dataframe_to_table(name=table_name, dataframe=obj)

    def load_input(self, context: dg.InputContext):
        if context.upstream_output:
            return read_dataframe_from_table(name=context.upstream_output.name)

@dg.input_manager
def my_subselection_input_manager():
    return read_dataframe_from_table(name="table_1")

@dg.op
def op1():
    """Do stuff."""

@dg.op(ins={"dataframe": dg.In(input_manager_key="my_input_manager")})
def op2(dataframe):
    """Do stuff."""
    dataframe.head()

@dg.job(
    resource_defs={
        "io_manager": MyIOManager(),
        "my_input_manager": my_subselection_input_manager,
    }
)
def my_subselection_job():
    op2(op1())

So far, this is set up so that `op2` always loads `table_1` even if you execute the full job. This would let you debug `op2`, but if you want to write this so that `op2` only loads `table_1` when no input is provided from an upstream op, you can rewrite the input manager as a subclass of the IO manager used for the rest of the job as follows:
codeBlockLines_e6Vv

class MyNewInputLoader(MyIOManager):
    def load_input(self, context: dg.InputContext):
        if context.upstream_output is None:
            # load input from table since there is no upstream output
            return read_dataframe_from_table(name="table_1")
        else:
            return super().load_input(context)

Now, when running the full job, `op2`'s input will be loaded using the IO manager on the output of `op1`. When running the job subset, `op2`'s input has no upstream output, so `table_1` will be loaded.
codeBlockLines_e6Vv
my_subselection_job.execute_in_process(
    op_selection=["op2"],
)

## Relevant APIs [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`@dg.dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader) | The decorator used to define a Dagster Type Loader. |
| [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) | The base class used to specify how to load inputs that depends on the type. |
| [`DagsterTypeLoaderContext`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoaderContext) | The context object provided to the function decorated by `@dagster_type_loader`. |
| [`@dg.input_manager`](https://docs.dagster.io/api/dagster/io-managers#dagster.input_manager) | The decorator used to define an input manager. |
| [`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager) | The base class used to specify how to load inputs. |

- [Working with Dagster types](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#working-with-dagster-types)
  - [Loading a built-in Dagster type from config](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#loading-a-built-in-dagster-type-from-config)
  - [Loading a custom Dagster type from config](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#loading-a-custom-dagster-type-from-config)
- [Working with input managers](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#working-with-input-managers)
  - [Providing an input manager for unconnected inputs](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#providing-an-input-manager-for-unconnected-inputs)
  - [Providing per-input config to input managers](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#providing-per-input-config-to-input-managers)
  - [Using input managers with subselection](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#using-input-managers-with-subselection)
- [Relevant APIs](https://docs.dagster.io/guides/build/jobs/unconnected-inputs#relevant-apis)


### File: curated_dagster_docs/foundation/orchestration_automation/schedules_time/defining-schedules.md
markdown

On this page

## Defining basic schedules [​](https://docs.dagster.io/guides/automate/schedules/defining-schedules\#defining-basic-schedules "Direct link to Defining basic schedules")

The following examples demonstrate how to define some basic schedules.

- Using ScheduleDefinition
- Using @schedule

This example demonstrates how to define a schedule using [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) that will run a job every day at midnight. While this example uses op jobs, the same approach will work with [asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs).
codeBlockLines_e6Vv
@dg.job
def my_job(): ...

basic_schedule = dg.ScheduleDefinition(job=my_job, cron_schedule="0 0 * * *")

note

The `cron_schedule` argument accepts standard [cron expressions](https://en.wikipedia.org/wiki/Cron). If your `croniter` dependency's version is `>= 1.0.12`, the argument will also accept the following:

- `@daily`
- `@hourly`
- `@monthly`

This example demonstrates how to define a schedule using [`@dg.schedule`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.schedule), which provides more flexibility than [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition). For example, you can [configure job behavior based on its scheduled run time](https://docs.dagster.io/guides/automate/schedules/configuring-job-behavior) or [emit log messages](https://docs.dagster.io/guides/automate/schedules/defining-schedules#emitting-log-messages-from-schedule-evaluation).
codeBlockLines_e6Vv
@schedule(job=my_job, cron_schedule="0 0 * * *")
def basic_schedule(): ...
  # things the schedule does, like returning a RunRequest or SkipReason

note

The `cron_schedule` argument accepts standard [cron expressions](https://en.wikipedia.org/wiki/Cron). If your `croniter` dependency's version is `>= 1.0.12`, the argument will also accept the following:

- `@daily`
- `@hourly`
- `@monthly`

## Emitting log messages from schedule evaluation [​](https://docs.dagster.io/guides/automate/schedules/defining-schedules\#emitting-log-messages-from-schedule-evaluation "Direct link to Emitting log messages from schedule evaluation")

This example demonstrates how to emit log messages from a schedule during its evaluation function. These logs will be visible in the UI when you inspect a tick in the schedule's tick history.
codeBlockLines_e6Vv
@dg.schedule(job=my_job, cron_schedule="* * * * *")
def logs_then_skips(context):
    context.log.info("Logging from a dg.schedule!")
    return dg.SkipReason("Nothing to do")

note

Schedule logs are stored in your [Dagster instance's compute log storage](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#compute-log-storage). You should ensure that your compute log storage is configured to view your schedule logs.

- [Defining basic schedules](https://docs.dagster.io/guides/automate/schedules/defining-schedules#defining-basic-schedules)
- [Emitting log messages from schedule evaluation](https://docs.dagster.io/guides/automate/schedules/defining-schedules#emitting-log-messages-from-schedule-evaluation)


### File: curated_dagster_docs/foundation/orchestration_automation/schedules_time/partitioning-assets.md
markdown

On this page

In Dagster, partitioning is a powerful technique for managing large datasets, improving pipeline performance, and enabling incremental processing. This guide will help you understand how to implement data partitioning in your Dagster projects.

There are several ways to partition your data in Dagster:

- [Time-based partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#time-based), for processing data in specific time intervals
- [Static partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#static-partitions), for dividing data based on predefined categories
- [Two-dimensional partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#two-dimensional-partitions), for partitioning data along two different axes simultaneously
- [Dynamic partitioning](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#dynamic-partitions), for creating partitions based on runtime information

note

We recommend limiting the number of partitions for each asset to 25,000 or fewer. Assets with partition counts exceeding this limit will likely have slower load times in the UI.

## Time-based partitions [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#time-based "Direct link to Time-based partitions")

A common use case for partitioning is to process data that can be divided into time intervals, such as daily logs or monthly reports.
codeBlockLines_e6Vv
import datetime
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition,
# which will create a range of partitions from
# 2024-01-01 to the day before the current time
daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")

# Define the partitioned asset
@dg.asset(partitions_def=daily_partitions)
def daily_sales_data(context: dg.AssetExecutionContext) -> None:
    date = context.partition_key
    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "date": [date] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/daily_sales", exist_ok=True)
    filename = f"data/daily_sales/sales_{date}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Daily sales data written to {filename}")

@dg.asset(
    partitions_def=daily_partitions,  # Use the daily partitioning scheme
    deps=[daily_sales_data],  # Define dependency on daily_sales_data asset
)
def daily_sales_summary(context):
    partition_date_str = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/daily_sales/sales_{partition_date_str}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "date": partition_date_str,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Daily sales summary for {partition_date_str}: {summary}")

# Create a partitioned asset job
daily_sales_job = dg.define_asset_job(
    name="daily_sales_job",
    selection=[daily_sales_data, daily_sales_summary],
)

# Create a schedule to run the job daily
@dg.schedule(
    job=daily_sales_job,
    cron_schedule="0 1 * * *",  # Run at 1:00 AM every day
)
def daily_sales_schedule(context):
    """Process previous day's sales data."""
    # Calculate the previous day's date
    previous_day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    date = previous_day.strftime("%Y-%m-%d")
    return dg.RunRequest(
        run_key=date,
        partition_key=date,
    )

# Define the Definitions object
defs = dg.Definitions(
    assets=[daily_sales_data, daily_sales_summary],
    jobs=[daily_sales_job],
    schedules=[daily_sales_schedule],
)

## Partitions with predefined categories [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#static-partitions "Direct link to Partitions with predefined categories")

Sometimes you have a set of predefined categories for your data. For instance, you might want to process data separately for different regions.
codeBlockLines_e6Vv
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])

# Define the partitioned asset
@dg.asset(partitions_def=region_partitions)  # Use the region partitioning scheme
def regional_sales_data(context: dg.AssetExecutionContext) -> None:
    region = context.partition_key

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/regional_sales", exist_ok=True)
    filename = f"data/regional_sales/sales_{region}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Regional sales data written to {filename}")

@dg.asset(
    partitions_def=region_partitions,  # Use the region partitioning scheme
    deps=[regional_sales_data],
)
def daily_sales_summary(context):
    region = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/regional_sales/sales_{region}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Regional sales summary for {region}: {summary}")

# Create a partitioned asset job
regional_sales_job = dg.define_asset_job(
    name="regional_sales_job",
    selection=[regional_sales_data, daily_sales_summary],
)

# Define the Definitions object
defs = dg.Definitions(
    assets=[regional_sales_data, daily_sales_summary],
    jobs=[regional_sales_job],
)

## Two-dimensional partitions [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#two-dimensional-partitions "Direct link to Two-dimensional partitions")

Two-dimensional partitioning allows you to partition data along two different axes simultaneously. This is useful when you need to process data that can be categorized in multiple ways. For example:
codeBlockLines_e6Vv
import datetime
import os

import pandas as pd

import dagster as dg

# Create two PartitionDefinitions
daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")
region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])
two_dimensional_partitions = dg.MultiPartitionsDefinition(
    {"date": daily_partitions, "region": region_partitions}
)

# Define the partitioned asset
@dg.asset(partitions_def=two_dimensional_partitions)
def daily_regional_sales_data(context: dg.AssetExecutionContext) -> None:
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "date": [date] * 10,
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/daily_regional_sales", exist_ok=True)
    filename = f"data/daily_regional_sales/sales_{context.partition_key}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Daily sales data written to {filename}")

@dg.asset(
    partitions_def=two_dimensional_partitions,
    deps=[daily_regional_sales_data],
)
def daily_regional_sales_summary(context):
    # partition_key looks like "2024-01-01|us"
    keys_by_dimension: dg.MultiPartitionKey = context.partition_key.keys_by_dimension

    date = keys_by_dimension["date"]
    region = keys_by_dimension["region"]

    filename = f"data/daily_regional_sales/sales_{context.partition_key}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "date": date,
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Daily sales summary for {context.partition_key}: {summary}")

# Create a partitioned asset job
daily_regional_sales_job = dg.define_asset_job(
    name="daily_regional_sales_job",
    selection=[daily_regional_sales_data, daily_regional_sales_summary],
)

# Create a schedule to run the job daily
@dg.schedule(
    job=daily_regional_sales_job,
    cron_schedule="0 1 * * *",  # Run at 1:00 AM every day
)
def daily_regional_sales_schedule(context):
    """Process previous day's sales data for all regions."""
    previous_day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    date = previous_day.strftime("%Y-%m-%d")

    # Create a run request for each region (3 runs in total every day)
    return [\
        dg.RunRequest(\
            run_key=f"{date}|{region}",\
            partition_key=dg.MultiPartitionKey({"date": date, "region": region}),\
        )\
        for region in region_partitions.get_partition_keys()\
    ]

# Define the Definitions object
defs = dg.Definitions(
    assets=[daily_regional_sales_data, daily_regional_sales_summary],
    jobs=[daily_regional_sales_job],
    schedules=[daily_regional_sales_schedule],
)

In this example:

- Using `MultiPartitionsDefinition`, the `two_dimensional_partitions` is defined with two dimensions: `date` and `region`
- The partition key would be: `2024-08-01|us`
- The `daily_regional_sales_data` and `daily_regional_sales_summary` assets are defined with the same two-dimensional partitioning scheme
- The `daily_regional_sales_schedule` runs daily at 1:00 AM, processing the previous day's data for all regions. It uses `MultiPartitionKey` to specify partition keys for both date and region dimensions, resulting in three runs per day, one for each region.

## Partitions with dynamic categories [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#dynamic-partitions "Direct link to Partitions with dynamic categories")

Sometimes you don't know the partitions in advance. For example, you might want to process new regions that are added in your system. In these cases, you can use dynamic partitioning to create partitions based on runtime information.

Consider this example:

Dynamic partitioning
codeBlockLines_e6Vv
import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition
region_partitions = dg.DynamicPartitionsDefinition(name="regions")

# Define the partitioned asset
@dg.asset(partitions_def=region_partitions)
def regional_sales_data(context: dg.AssetExecutionContext) -> None:
    region = context.partition_key

    # Simulate fetching daily sales data
    df = pd.DataFrame(
        {
            "region": [region] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

    os.makedirs("data/regional_sales", exist_ok=True)
    filename = f"data/regional_sales/sales_{region}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Regional sales data written to {filename}")

@dg.asset(
    partitions_def=region_partitions,
    deps=[regional_sales_data],
)
def daily_sales_summary(context):
    region = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"data/regional_sales/sales_{region}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Regional sales summary for {region}: {summary}")

# Create a partitioned asset job
regional_sales_job = dg.define_asset_job(
    name="regional_sales_job",
    selection=[regional_sales_data, daily_sales_summary],
)

@dg.sensor(job=regional_sales_job)
def all_regions_sensor(context: dg.SensorEvaluationContext):
    # Simulate fetching all regions from an external system
    all_regions = ["us", "eu", "jp", "ca", "uk", "au"]

    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=region) for region in all_regions],
        dynamic_partitions_requests=[region_partitions.build_add_request(all_regions)],
    )

# Define the Definitions object
defs = dg.Definitions(
    assets=[regional_sales_data, daily_sales_summary],
    jobs=[regional_sales_job],
    sensors=[all_regions_sensor],
)

In this example:

- Because the partition values are unknown in advance, `DynamicPartitionsDefinition` is used to define `region_partitions`
- When triggered, the `all_regions_sensor` will dynamically add all regions to the partition set. Once it kicks off runs, it will dynamically kick off runs for all regions. In this example, that would be six times; one for each region.

## Materializing partitioned assets [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#materializing-partitioned-assets "Direct link to Materializing partitioned assets")

When you materialize a partitioned asset, you choose which partitions to materialize and Dagster will launch a run for each partition.

note

If you choose more than one partition, the [Dagster daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) needs to be running to queue the multiple runs.

The following image shows the **Launch runs** dialog on an asset's **Details** page, where you'll be prompted to select a partition to materialize:

![Rematerialize partition](https://docs.dagster.io/assets/images/rematerialize-partition-a57c886d22ee57191eff6e1a794af7e2.png)

After a partition has been successfully materialized, it will display as green in the partitions bar:

![Successfully materialized partition](https://docs.dagster.io/assets/images/materialized-partitioned-asset-360546ca8f4a4a845de9c27c9b8562fa.png)

## Viewing materializations by partition [​](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets\#viewing-materializations-by-partition "Direct link to Viewing materializations by partition")

To view materializations by partition for a specific asset, navigate to the **Activity** tab of the asset's **Details** page:

![Asset activity section of asset details page](https://docs.dagster.io/assets/images/materialized-partitioned-asset-activity-5269cab8c25188337b6f56af25cd906d.png)

- [Time-based partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#time-based)
- [Partitions with predefined categories](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#static-partitions)
- [Two-dimensional partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#two-dimensional-partitions)
- [Partitions with dynamic categories](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#dynamic-partitions)
- [Materializing partitioned assets](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#materializing-partitioned-assets)
- [Viewing materializations by partition](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets#viewing-materializations-by-partition)


### File: curated_dagster_docs/foundation/orchestration_automation/sensors_event/asset-sensors.md
markdown

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
codeBlockLines_e6Vv
import dagster as dg

@dg.asset
def daily_sales_data(context: dg.AssetExecutionContext):
    context.log.info("Asset to watch")

@dg.asset
def weekly_report(context: dg.AssetExecutionContext):
    context.log.info("Asset to trigger")

# Job that materializes the weekly_report asset
my_job = dg.define_asset_job("my_job", [weekly_report])

# Trigger my_job when the daily_sales_data asset is materialized
@dg.asset_sensor(asset_key=dg.AssetKey("daily_sales_data"), job_name="my_job")
def daily_sales_data_sensor():
    return dg.RunRequest()

defs = dg.Definitions(
    assets=[daily_sales_data, weekly_report],
    jobs=[my_job],
    sensors=[daily_sales_data_sensor],
)

## Customizing the evaluation function of an asset sensor [​](https://docs.dagster.io/guides/automate/asset-sensors\#customizing-the-evaluation-function-of-an-asset-sensor "Direct link to Customizing the evaluation function of an asset sensor")

You can customize the evaluation function of an asset sensor to include specific logic for deciding when to trigger a run. This allows for fine-grained control over the conditions under which downstream jobs are executed.

AssetMaterialization

User Evaluation Function

RunRequest

SkipReason

In the following example, the `@asset_sensor` decorator defines a custom evaluation function that returns a `RunRequest` object when the asset is materialized and certain metadata is present, otherwise it skips the run.
codeBlockLines_e6Vv
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

## Triggering a job with custom configuration [​](https://docs.dagster.io/guides/automate/asset-sensors\#triggering-a-job-with-custom-configuration "Direct link to Triggering a job with custom configuration")

By providing a configuration to the `RunRequest` object, you can trigger a job with a specific configuration. This is useful when you want to trigger a job with custom parameters based on custom logic you define.

For example, you might use a sensor to trigger a job when an asset is materialized, but also pass metadata about that materialization to the job:
codeBlockLines_e6Vv
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

## Monitoring multiple assets [​](https://docs.dagster.io/guides/automate/asset-sensors\#monitoring-multiple-assets "Direct link to Monitoring multiple assets")

note

The `@multi_asset_sensor` has been marked as deprecated, but will not be removed from the codebase until Dagster 2.0 is released, meaning it will continue to function as it currently does for the foreseeable future. Its functionality has been largely superseded by the `AutomationCondition` system. For more information, see the [Declarative Automation documentation](https://docs.dagster.io/guides/automate/declarative-automation/).

When building a pipeline, you may want to monitor multiple assets with a single sensor. This can be accomplished with a multi-asset sensor.

The following example uses a `@multi_asset_sensor` to monitor two assets that triggers an asset job once both have been materialized. You can also trigger op jobs this way.
codeBlockLines_e6Vv
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

## Next steps [​](https://docs.dagster.io/guides/automate/asset-sensors\#next-steps "Direct link to Next steps")

- Explore [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation/) as an alternative to asset sensors

- [Getting started](https://docs.dagster.io/guides/automate/asset-sensors#getting-started)
- [Cross-job and cross-code location dependencies](https://docs.dagster.io/guides/automate/asset-sensors#cross-job-and-cross-code-location-dependencies)
- [Customizing the evaluation function of an asset sensor](https://docs.dagster.io/guides/automate/asset-sensors#customizing-the-evaluation-function-of-an-asset-sensor)
- [Triggering a job with custom configuration](https://docs.dagster.io/guides/automate/asset-sensors#triggering-a-job-with-custom-configuration)
- [Monitoring multiple assets](https://docs.dagster.io/guides/automate/asset-sensors#monitoring-multiple-assets)
- [Next steps](https://docs.dagster.io/guides/automate/asset-sensors#next-steps)


### File: curated_dagster_docs/foundation/orchestration_automation/sensors_event/run-status-sensors.md
markdown

If you want to act on the status of a run, Dagster provides a way to create a sensor that reacts to run statuses. You can use [`run_status_sensor`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.run_status_sensor) with a specified [`DagsterRunStatus`](https://docs.dagster.io/api/dagster/internals#dagster.DagsterRunStatus) to decorate a function that will run when the given status occurs. This can be used to launch other runs, send alerts to a monitoring service on run failure, or report a run success.

Here is an example of a run status sensor that launches a run of `status_reporting_job` if a run is successful:
codeBlockLines_e6Vv
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    request_job=status_reporting_job,
)
def report_status_sensor(context: dg.RunStatusSensorContext):
    # this condition prevents the sensor from triggering status_reporting_job again after it succeeds
    if context.dagster_run.job_name != status_reporting_job.name:
        run_config = {
            "ops": {
                "status_report": {"config": {"job_name": context.dagster_run.job_name}}
            }
        }
        return dg.RunRequest(run_key=None, run_config=run_config)
    else:
        return dg.SkipReason("Don't report status of status_reporting_job")

`request_job` is the job that will be run when the `RunRequest` is returned.

Note that in `report_status_sensor` we conditionally return a `RunRequest`. This ensures that when `report_status_sensor` runs `status_reporting_job` it doesn't enter an infinite loop where the success of `status_reporting_job` triggers another run of `status_reporting_job`, which triggers another run, and so on.

Here is an example of a sensor that reports job success in a Slack message:
codeBlockLines_e6Vv
from dagster import run_status_sensor, RunStatusSensorContext, DagsterRunStatus

@run_status_sensor(run_status=DagsterRunStatus.SUCCESS)
def my_slack_on_run_success(context: RunStatusSensorContext):
    slack_client = WebClient(token=os.environ["SLACK_DAGSTER_ETL_BOT_TOKEN"])

    slack_client.chat_postMessage(
        channel="#alert-channel",
        text=f'Job "{context.dagster_run.job_name}" succeeded.',
    )

When a run status sensor is triggered by a run but doesn't return anything, Dagster will report an event back to the run to indicate that the sensor ran.

Once you have written your sensor, you can add the sensor to a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object so it can be enabled and used the same as other sensors:
codeBlockLines_e6Vv
from dagster import Definitions

defs = Definitions(jobs=[my_sensor_job], sensors=[my_slack_on_run_success])



### File: curated_dagster_docs/foundation/orchestration_automation/sensors_event/sensors.md
markdown

On this page

Sensors enable you to take action in response to events that occur either internally within Dagster or in external systems. They check for events at regular intervals and either perform an action or provide an explanation for why the action was skipped.

Examples of events include:

- a run completes in Dagster
- a run fails in Dagster
- a job materializes a specific asset
- a file appears in an s3 bucket
- an external system is down

Examples of actions include:

- launching a run
- sending a Slack message
- inserting a row into a database

tip

An alternative to polling with sensors is to push events to Dagster using the [Dagster API](https://docs.dagster.io/guides/operate/graphql/).

Prerequisites

To follow the steps in this guide, you'll need:

- Familiarity with [assets](https://docs.dagster.io/guides/build/assets/)
- Familiarity with [jobs](https://docs.dagster.io/guides/build/jobs/)

## Basic sensor [​](https://docs.dagster.io/guides/automate/sensors\#basic-sensor "Direct link to Basic sensor")

Sensors are defined with the `@sensor` decorator. The following example includes a `check_for_new_files` function that simulates finding new files. In a real scenario, this function would check an actual system or directory.

If the sensor finds new files, it starts a run of `my_job`. If not, it skips the run and logs `No new files found` in the Dagster UI.
codeBlockLines_e6Vv
import random
from typing import List

import dagster as dg

# Define the asset
@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("Hello, world!")

# Define asset job
my_job = dg.define_asset_job("my_job", selection=[my_asset])

# Define file check
def check_for_new_files() -> list[str]:
    if random.random() > 0.5:
        return ["file1", "file2"]
    return []

# Define the sensor
@dg.sensor(
    job=my_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,  # Sensor is turned on by default
)
def new_file_sensor():
    new_files = check_for_new_files()
    # New files, run my_job
    if new_files:
        for filename in new_files:
            yield dg.RunRequest(run_key=filename)
    # No new files, skip the run and log the reason
    else:
        yield dg.SkipReason("No new files found")

defs = dg.Definitions(assets=[my_asset], jobs=[my_job], sensors=[new_file_sensor])

tip

Unless a sensor has a `default_status` of `DefaultSensorStatus.RUNNING`, it won't be enabled when first deployed to a Dagster instance. To find and enable the sensor, click **Automation > Sensors** in the Dagster UI.

To explicitly disable a sensor, you can use `DefaultSensorStatus.STOPPED`.

## Customizing intervals between evaluations [​](https://docs.dagster.io/guides/automate/sensors\#customizing-intervals-between-evaluations "Direct link to Customizing intervals between evaluations")

The `minimum_interval_seconds` argument allows you to specify the minimum number of seconds that will elapse between sensor evaluations. This means that the sensor won't be evaluated more frequently than the specified interval.

It's important to note that this interval represents a minimum interval between runs of the sensor and not the exact frequency the sensor runs. If a sensor takes longer to complete than the specified interval, the next evaluation will be delayed accordingly.
codeBlockLines_e6Vv
# Sensor will be evaluated at least every 30 seconds
@dg.sensor(job=my_job, minimum_interval_seconds=30)
def new_file_sensor():
  ...

In this example, if the `new_file_sensor`'s evaluation function takes less than a second to run, you can expect the sensor to run consistently around every 30 seconds. However, if the evaluation function takes longer, the interval between evaluations will be longer.

## Preventing duplicate runs [​](https://docs.dagster.io/guides/automate/sensors\#preventing-duplicate-runs "Direct link to Preventing duplicate runs")

To prevent duplicate runs, you can use run keys to uniquely identify each `RunRequest`. In the [previous example](https://docs.dagster.io/guides/automate/sensors#basic-sensor), the `RunRequest` was constructed with a `run_key`:
codeBlockLines_e6Vv
yield dg.RunRequest(run_key=filename)

For a given sensor, a single run is created for each `RunRequest` with a unique `run_key`. Dagster will skip processing requests with previously used run keys, ensuring that duplicate runs won't be created.

## Cursors and high volume events [​](https://docs.dagster.io/guides/automate/sensors\#cursors-and-high-volume-events "Direct link to Cursors and high volume events")

When dealing with a large number of events, you may want to implement a cursor to optimize sensor performance. Unlike run keys, cursors allow you to implement custom logic that manages state.

The following example demonstrates how you might use a cursor to only create `RunRequests` for files in a directory that have been updated since the last time the sensor ran.
codeBlockLines_e6Vv
import os

import dagster as dg

MY_DIRECTORY = "data"

@dg.asset
def my_asset(context: dg.AssetExecutionContext):
    context.log.info("Hello, world!")

my_job = dg.define_asset_job("my_job", selection=[my_asset])

@dg.sensor(
    job=my_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
# Enable sensor context
def updated_file_sensor(context):
    # Get current cursor value from sensor context
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime

    # Loop through directory
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            # Get the file's last modification time (st_mtime)
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime

            # If the file was updated since the last eval time, continue
            if file_mtime <= last_mtime:
                continue

            # Construct the RunRequest with run_key and config
            run_key = f"{filename}:{file_mtime}"
            run_config = {"ops": {"my_asset": {"config": {"filename": filename}}}}
            yield dg.RunRequest(run_key=run_key, run_config=run_config)

            # Keep the larger value of max_mtime and file last updated
            max_mtime = max(max_mtime, file_mtime)

    # Update the cursor
    context.update_cursor(str(max_mtime))

defs = dg.Definitions(assets=[my_asset], jobs=[my_job], sensors=[updated_file_sensor])

For sensors that consume multiple event streams, you may need to serialize and deserialize a more complex data structure in and out of the cursor string to keep track of the sensor's progress over the multiple streams.

note

The preceding example uses both a `run_key` and a cursor, which means that if the cursor is reset but the files don't change, new runs won't be launched. This is because the run keys associated with the files won't change.

If you want to be able to reset a sensor's cursor, don't set `run_key` s on `RunRequest` s.

## Next steps [​](https://docs.dagster.io/guides/automate/sensors\#next-steps "Direct link to Next steps")

By understanding and effectively using these automation methods, you can build more efficient data pipelines that respond to your specific needs and constraints.

- Run pipelines on a [schedule](https://docs.dagster.io/guides/automate/schedules)
- Trigger cross-job dependencies with [asset sensors](https://docs.dagster.io/guides/automate/asset-sensors)
- Explore [Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation) as an alternative to sensors

- [Basic sensor](https://docs.dagster.io/guides/automate/sensors#basic-sensor)
- [Customizing intervals between evaluations](https://docs.dagster.io/guides/automate/sensors#customizing-intervals-between-evaluations)
- [Preventing duplicate runs](https://docs.dagster.io/guides/automate/sensors#preventing-duplicate-runs)
- [Cursors and high volume events](https://docs.dagster.io/guides/automate/sensors#cursors-and-high-volume-events)
- [Next steps](https://docs.dagster.io/guides/automate/sen