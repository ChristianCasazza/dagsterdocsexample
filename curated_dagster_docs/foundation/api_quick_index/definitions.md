
`class` dagster.Definitions  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L313)

A set of definitions explicitly available and loadable by Dagster tools.

Parameters:

- **assets** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _,_ _CacheableAssetsDefinition_ _\]_ _\]_ _\]_) – A list of assets. Assets can be created by annotating a function with [`@asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) or [`@observable_source_asset`](https://docs.dagster.io/api/dagster/assets#dagster.observable_source_asset). Or they can by directly instantiating [`AssetsDefinition`](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition), [`SourceAsset`](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset), or `CacheableAssetsDefinition`.
- **asset\_checks** ( _Optional_ _\[_ _Iterable_ _\[_ [_AssetChecksDefinition_](https://docs.dagster.io/api/dagster/asset-checks#dagster.AssetChecksDefinition) _\]_ _\]_) – A list of asset checks.
- **schedules** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_ScheduleDefinition_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) _,_ _UnresolvedPartitionedAssetScheduleDefinition_ _\]_ _\]_ _\]_) – List of schedules.
- **sensors** ( _Optional_ _\[_ _Iterable_ _\[_ [_SensorDefinition_](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SensorDefinition) _\]_ _\]_) – List of sensors, typically created with [`@sensor`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.sensor).
- **jobs** ( _Optional_ _\[_ _Iterable_ _\[_ _Union_ _\[_ [_JobDefinition_](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) _,_ _UnresolvedAssetJobDefinition_ _\]_ _\]_ _\]_) – List of jobs. Typically created with [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) or with [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) for jobs defined in terms of ops directly. Jobs created with [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) must already have resources bound at job creation time. They do not respect the resources argument here.
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets. The resources dictionary takes raw Python objects, not just instances of [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). If that raw object inherits from [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager), it gets coerced to an [`IOManagerDefinition`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManagerDefinition). Any other object is coerced to a [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). These resources will be automatically bound to any assets passed to this Definitions instance using [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources). Assets passed to Definitions with resources already bound using [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources) will override this dictionary.
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _,_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _\]_ _\]_) – Default executor for jobs. Individual jobs can override this and define their own executors by setting the executor on [`@job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`define_asset_job`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job) explicitly. This executor will also be used for materializing assets directly outside of the context of jobs. If an [`Executor`](https://docs.dagster.io/api/dagster/internals#dagster.Executor) is passed, it is coerced into an [`ExecutorDefinition`](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition).
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_) – Default loggers for jobs. Individual jobs can define their own loggers by setting them explictly.\
- **metadata** ( _Optional_ _\[_ _MetadataMapping_ _\]_) – Arbitrary metadata for the Definitions. Not displayed in the UI but accessible on the Definitions instance at runtime.\
\
Example usage:\
\
```codeBlockLines_e6Vv\
defs = Definitions(\
    assets=[asset_one, asset_two],\
    schedules=[a_schedule],\
    sensors=[a_sensor],\
    jobs=[a_job],\
    resources={\
        "a_resource": some_resource,\
    },\
    asset_checks=[asset_one_check_one]\
)\
\
```\
\
Dagster separates user-defined code from system tools such the web server and\
the daemon. Rather than loading code directly into process, a tool such as the\
webserver interacts with user-defined code over a serialization boundary.\
\
These tools must be able to locate and load this code when they start. Via CLI\
arguments or config, they specify a Python module to inspect.\
\
A Python module is loadable by Dagster tools if there is a top-level variable\
that is an instance of [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
\
`static` merge  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L591)\
\
Merges multiple Definitions objects into a single Definitions object.\
\
The returned Definitions object has the union of all the definitions in the input\
Definitions objects.\
\
Raises an error if the Definitions objects to be merged contain conflicting values for the\
same resource key or logger key, or if they have different executors defined.\
\
Examples:\
\
```codeBlockLines_e6Vv\
import submodule1\
import submodule2\
\
defs = Definitions.merge(submodule1.defs, submodule2.defs)\
\
```\
\
Returns: The merged definitions.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
`static` validate\_loadable  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L576)\
\
Validates that the enclosed definitions will be loadable by Dagster:\
\
- No assets have conflicting keys.\
- No jobs, sensors, or schedules have conflicting names.\
- All asset jobs can be resolved.\
- All resource requirements are satisfied.\
\
Meant to be used in unit tests.\
\
Raises an error if any of the above are not true.\
\
get\_all\_asset\_specs  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L675)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Returns an AssetSpec object for every asset contained inside the Definitions object.\
\
get\_asset\_value\_loader  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L502)\
\
Returns an object that can load the contents of assets as Python objects.\
\
Invokes load\_input on the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) associated with the assets. Avoids\
spinning up resources separately for each asset.\
\
Usage:\
\
```codeBlockLines_e6Vv\
with defs.get_asset_value_loader() as loader:\
    asset1 = loader.load_asset_value("asset1")\
    asset2 = loader.load_asset_value("asset2")\
\
```\
\
get\_job\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L438)\
\
Get a job definition by name. If you passed in a an `UnresolvedAssetJobDefinition`\
(return value of [`define_asset_job()`](https://docs.dagster.io/api/dagster/assets#dagster.define_asset_job)) it will be resolved to a [`JobDefinition`](https://docs.dagster.io/api/dagster/jobs#dagster.JobDefinition) when returned\
from this function, with all resource dependencies fully resolved.\
\
get\_schedule\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L456)\
\
Get a [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) by name.\
If your passed-in schedule had resource dependencies, or the job targeted by the schedule had\
resource dependencies, those resource dependencies will be fully resolved on the returned object.\
\
get\_sensor\_def  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L447)\
\
Get a [`SensorDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.SensorDefinition) by name.\
If your passed-in sensor had resource dependencies, or the job targeted by the sensor had\
resource dependencies, those resource dependencies will be fully resolved on the returned object.\
\
load\_asset\_value  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L465)\
\
Load the contents of an asset as a Python object.\
\
Invokes load\_input on the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) associated with the asset.\
\
If you want to load the values of multiple assets, it’s more efficient to use\
[`get_asset_value_loader()`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions.get_asset_value_loader), which avoids spinning up\
resources separately for each asset.\
\
Parameters:\
\
- **asset\_key** ( _Union_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _str_ _\]_) – The key of the asset to load.\
- **python\_type** ( _Optional_ _\[_ _Type_ _\]_) – The python type to load the asset as. This is what will be returned inside load\_input by context.dagster\_type.typing\_type.\
- **partition\_key** ( _Optional_ _\[_ _str_ _\]_) – The partition of the asset to load.\
- **metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Input metadata to pass to the [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) (is equivalent to setting the metadata argument in In or AssetIn).\
\
Returns: The contents of an asset as a Python object.\
\
map\_asset\_specs  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L711)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Map a function over the included AssetSpecs or AssetsDefinitions in this Definitions object, replacing specs in the sequence\
or specs in an AssetsDefinitions with the result of the function.\
\
Parameters:\
\
- **func** ( _Callable_ _\[_ _\[_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_ _,_ [_AssetSpec_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec) _\]_) – The function to apply to each AssetSpec.\
- **selection** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _,_ _Sequence_ _\[_ [_AssetKey_](https://docs.dagster.io/api/dagster/assets#dagster.AssetKey) _\]_ _,_ _Sequence_ _\[_ _Union_ _\[_ [_AssetsDefinition_](https://docs.dagster.io/api/dagster/assets#dagster.AssetsDefinition) _,_ [_SourceAsset_](https://docs.dagster.io/api/dagster/assets#dagster.SourceAsset) _\]_ _\]_ _,_ [_AssetSelection_](https://docs.dagster.io/api/dagster/assets#dagster.AssetSelection) _\]_ _\]_) – An asset selection to narrow down the set of assets to apply the function to. If not provided, applies to all assets.\
\
Returns: A Definitions object where the AssetSpecs have been replaced with the result of the function where the selection applies.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
Examples:\
\
```codeBlockLines_e6Vv\
import dagster as dg\
\
my_spec = dg.AssetSpec("asset1")\
\
@dg.asset\
def asset2(_): ...\
\
defs = Definitions(\
    assets=[asset1, asset2]\
)\
\
# Applies to asset1 and asset2\
mapped_defs = defs.map_asset_specs(\
    func=lambda s: s.merge_attributes(metadata={"new_key": "new_value"}),\
)\
\
# Applies only to asset1\
mapped_defs = defs.map_asset_specs(\
    func=lambda s: s.replace_attributes(metadata={"new_key": "new_value"}),\
    selection="asset1",\
)\
\
```\
\
dagster.create\_repository\_using\_definitions\_args  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/definitions_class.py#L52)\
\
Create a named repository using the same arguments as [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). In older\
versions of Dagster, repositories were the mechanism for organizing assets, schedules, sensors,\
and jobs. There could be many repositories per code location. This was a complicated ontology but\
gave users a way to organize code locations that contained large numbers of heterogenous definitions.\
\
As a stopgap for those who both want to 1) use the new [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) API and 2) but still\
want multiple logical groups of assets in the same code location, we have introduced this function.\
\
Example usage:\
\
```codeBlockLines_e6Vv\
named_repo = create_repository_using_definitions_args(\
    name="a_repo",\
    assets=[asset_one, asset_two],\
    schedules=[a_schedule],\
    sensors=[a_sensor],\
    jobs=[a_job],\
    resources={\
        "a_resource": some_resource,\
    }\
)\
\
```\
\
dagster.load\_definitions\_from\_current\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L78)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the module where this function is called.\
\
Parameters:\
\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the current module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L49)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given module.\
\
Parameters:\
\
- **module** ( _ModuleType_) – The Python module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_modules  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L17)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given modules.\
\
Parameters:\
\
- **modules** ( _Iterable_ _\[_ _ModuleType_ _\]_) – The Python modules to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given modules.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_package\_module  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L110)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the given package module.\
\
Parameters:\
\
- **package\_module** ( _ModuleType_) – The package module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the given package module.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
dagster.load\_definitions\_from\_package\_name  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/module_loaders/load_defs_from_module.py#L143)\
\
preview\
\
This API is currently in preview, and may have breaking changes in patch version releases. This API is not considered ready for production use.\
\
Constructs the [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) from the package module for the given package name.\
\
Parameters:\
\
- **package\_name** ( _str_) – The name of the package module to look for [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) inside.\
- **resources** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Dictionary of resources to bind to assets in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions).\
- **loggers** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ [_LoggerDefinition_](https://docs.dagster.io/api/dagster/loggers#dagster.LoggerDefinition) _\]_ _\]_) – Default loggers for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own loggers by setting them explicitly.\
- **executor** ( _Optional_ _\[_ _Union_ _\[_ [_Executor_](https://docs.dagster.io/api/dagster/internals#dagster.Executor) _,_ [_ExecutorDefinition_](https://docs.dagster.io/api/dagster/internals#dagster.ExecutorDefinition) _\]_ _\]_) – Default executor for jobs in the loaded [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). Individual jobs can define their own executors by setting them explicitly.\
\
Returns: The [`dagster.Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) defined in the package module for the given package name.Return type: [Definitions](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions)\
\
