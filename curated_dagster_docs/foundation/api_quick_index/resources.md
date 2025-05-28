
On this page

## Pythonic resource system [​](https://docs.dagster.io/api/dagster/resources\#pythonic-resource-system "Direct link to Pythonic resource system")

The following classes are used as part of the new [Pythonic resources system](https://docs.dagster.io/guides/build/external-resources/).

`class` dagster.ConfigurableResource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/pythonic_config/resource.py#L595)

Base class for Dagster resources that utilize structured config.

This class is a subclass of both [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) and [`Config`](https://docs.dagster.io/api/dagster/config#dagster.Config).

Example definition:

```codeBlockLines_e6Vv
class WriterResource(ConfigurableResource):
    prefix: str

    def output(self, text: str) -> None:
        print(f"{self.prefix}{text}")

```

Example usage:

```codeBlockLines_e6Vv
@asset
def asset_that_uses_writer(writer: WriterResource):
    writer.output("text")

defs = Definitions(
    assets=[asset_that_uses_writer],
    resources={"writer": WriterResource(prefix="a_prefix")},
)

```

You can optionally use this class to model configuration only and vend an object
of a different type for use at runtime. This is useful for those who wish to
have a separate object that manages configuration and a separate object at runtime. Or
where you want to directly use a third-party class that you do not control.

To do this you override the create\_resource methods to return a different object.

```codeBlockLines_e6Vv
class WriterResource(ConfigurableResource):
    prefix: str

    def create_resource(self, context: InitResourceContext) -> Writer:
        # Writer is pre-existing class defined else
        return Writer(self.prefix)

```

Example usage:

```codeBlockLines_e6Vv
@asset
def use_preexisting_writer_as_resource(writer: ResourceParam[Writer]):
    writer.output("text")

defs = Definitions(
    assets=[use_preexisting_writer_as_resource],
    resources={"writer": WriterResource(prefix="a_prefix")},
)

```

`class` dagster.ResourceDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L57)

Core class for defining resources.

Resources are scoped ways to make external resources (like database connections) available to
ops and assets during job execution and to clean up after execution resolves.

If resource\_fn yields once rather than returning (in the manner of functions decorable with
`@contextlib.contextmanager`) then the body of the
function after the yield will be run after execution resolves, allowing users to write their
own teardown/cleanup logic.

Depending on your executor, resources may be instantiated and cleaned up more than once in a
job execution.

Parameters:

- **resource\_fn** ( _Callable_ _\[_ _\[_ [_InitResourceContext_](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) _\]_ _,_ _Any_ _\]_) – User-provided function to instantiate the resource, which will be made available to executions keyed on the `context.resources` object.
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema)) – The schema for the config. If set, Dagster will check that config provided for the resource matches this schema and fail if it does not. If not set, Dagster will accept any config provided for the resource.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.\
- **required\_resource\_keys** – (Optional\[Set\[str\]\]) Keys for the resources required by this resource. A DagsterInvariantViolationError will be raised during initialization if dependencies are cyclic.\
- **version** ( _Optional_ _\[_ _str_ _\]_) – beta (Beta) The version of the resource’s definition fn. Two wrapped resource functions should only have the same version if they produce the same resource definition when provided with the same inputs.\
\
`static` hardcoded\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L175)\
\
A helper function that creates a `ResourceDefinition` with a hardcoded object.\
\
Parameters:\
\
- **value** ( _Any_) – The value that will be accessible via context.resources.resource\_name.\
- **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.\
\
Returns: A hardcoded resource.Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` mock\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L197)\
\
A helper function that creates a `ResourceDefinition` which wraps a `mock.MagicMock`.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.Returns:\
A resource that creates the magic methods automatically and helps\
you mock existing resources.\
\
Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` none\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L162)\
\
A helper function that returns a none resource.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the resource. Defaults to None.Returns: A resource that does nothing.Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`static` string\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L215)\
\
Creates a `ResourceDefinition` which takes in a single string as configuration\
and returns this configured string to any ops or assets which depend on it.\
\
Parameters: **description** ( _\[_ _Optional_ _\[_ _str_ _\]_ _\]_) – The description of the string resource. Defaults to None.Returns:\
A resource that takes in a single string as configuration and\
returns that string.\
\
Return type: \[ [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\]\
\
`property` description\
\
A human-readable description of the resource.\
\
`property` required\_resource\_keys\
\
A set of the resource keys that this resource depends on. These keys will be made available\
to the resource’s init context during execution, and the resource will not be instantiated\
until all required resources are available.\
\
`property` version\
\
A string which can be used to identify a particular code version of a resource definition.\
\
`class` dagster.InitResourceContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/init.py#L18)\
\
The context object available as the argument to the initialization function of a [`dagster.ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
Users should not instantiate this object directly. To construct an InitResourceContext for testing purposes, use [`dagster.build_init_resource_context()`](https://docs.dagster.io/api/dagster/resources#dagster.build_init_resource_context).\
\
Example:\
\
```codeBlockLines_e6Vv\
from dagster import resource, InitResourceContext\
\
@resource\
def the_resource(init_context: InitResourceContext):\
    init_context.log.info("Hello, world!")\
\
```\
\
`property` instance\
\
The Dagster instance configured for the current execution context.\
\
`property` log\
\
The Dagster log manager configured for the current execution context.\
\
`property` log\_manager\
\
The log manager for this run of the job.\
\
`property` resource\_config\
\
The configuration data provided by the run config. The schema\
for this data is defined by the `config_field` argument to\
[`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
`property` resource\_def\
\
The definition of the resource currently being constructed.\
\
`property` resources\
\
The resources that are available to the resource that we are initializing.\
\
`property` run\
\
The dagster run to use. When initializing resources outside of execution context, this will be None.\
\
dagster.make\_values\_resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L418)\
\
A helper function that creates a `ResourceDefinition` to take in user-defined values.\
\
This is useful for sharing values between ops.\
\
Parameters: **\*\*kwargs** – Arbitrary keyword arguments that will be passed to the config schema of the\
returned resource definition. If not set, Dagster will accept any config provided for\
the resource.\
For example:\
\
```codeBlockLines_e6Vv\
@op(required_resource_keys={"globals"})\
def my_op(context):\
    print(context.resources.globals["my_str_var"])\
\
@job(resource_defs={"globals": make_values_resource(my_str_var=str, my_int_var=int)})\
def my_job():\
    my_op()\
\
```\
\
Returns: A resource that passes in user-defined values.Return type: [ResourceDefinition](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition)\
\
dagster.build\_init\_resource\_context  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/init.py#L257)\
\
Builds resource initialization context from provided parameters.\
\
`build_init_resource_context` can be used as either a function or context manager. If there is a\
provided resource to `build_init_resource_context` that is a context manager, then it must be\
used as a context manager. This function can be used to provide the context argument to the\
invocation of a resource.\
\
Parameters:\
\
- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – The resources to provide to the context. These can be either values or resource definitions.\
- **config** ( _Optional_ _\[_ _Any_ _\]_) – The resource config to provide to the context.\
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured for the context. Defaults to DagsterInstance.ephemeral().\
\
Examples:\
\
```codeBlockLines_e6Vv\
context = build_init_resource_context()\
resource_to_init(context)\
\
with build_init_resource_context(\
    resources={"foo": context_manager_resource}\
) as context:\
    resource_to_init(context)\
\
```\
\
dagster.build\_resources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/build_resources.py#L42)\
\
Context manager that yields resources using provided resource definitions and run config.\
\
This API allows for using resources in an independent context. Resources will be initialized\
with the provided run config, and optionally, dagster\_run. The resulting resources will be\
yielded on a dictionary keyed identically to that provided for resource\_defs. Upon exiting the\
context, resources will also be torn down safely.\
\
Parameters:\
\
- **resources** ( _Mapping_ _\[_ _str_ _,_ _Any_ _\]_) – Resource instances or definitions to build. All required resource dependencies to a given resource must be contained within this dictionary, or the resource build will fail.\
- **instance** ( _Optional_ _\[_ [_DagsterInstance_](https://docs.dagster.io/api/dagster/internals#dagster.DagsterInstance) _\]_) – The dagster instance configured to instantiate resources on.\
- **resource\_config** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – A dict representing the config to be provided to each resource during initialization and teardown.\
- **dagster\_run** ( _Optional_ _\[_ _PipelineRun_ _\]_) – The pipeline run to provide during resource initialization and teardown. If the provided resources require either the dagster\_run or run\_id attributes of the provided context during resource initialization and/or teardown, this must be provided, or initialization will fail.\
- **log\_manager** ( _Optional_ _\[_ [_DagsterLogManager_](https://docs.dagster.io/api/dagster/loggers#dagster.DagsterLogManager) _\]_) – Log Manager to use during resource initialization. Defaults to system log manager.\
- **event\_loop** ( _Optional_ _\[_ _AbstractEventLoop_ _\]_) – An event loop for handling resources with async context managers.\
\
Examples:\
\
```codeBlockLines_e6Vv\
from dagster import resource, build_resources\
\
@resource\
def the_resource():\
    return "foo"\
\
with build_resources(resources={"from_def": the_resource, "from_val": "bar"}) as resources:\
    assert resources.from_def == "foo"\
    assert resources.from_val == "bar"\
\
```\
\
dagster.with\_resources  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/with_resources.py#L15)\
\
Adds dagster resources to copies of resource-requiring dagster definitions.\
\
An error will be thrown if any provided definitions have a conflicting\
resource definition provided for a key provided to resource\_defs. Resource\
config can be provided, with keys in the config dictionary corresponding to\
the keys for each resource definition. If any definition has unsatisfied\
resource keys after applying with\_resources, an error will be thrown.\
\
Parameters:\
\
- **definitions** ( _Iterable_ _\[_ _ResourceAddable_ _\]_) – Dagster definitions to provide resources to.\
- **resource\_defs** ( _Mapping_ _\[_ _str_ _,_ _object_ _\]_) – Mapping of resource keys to objects to satisfy resource requirements of provided dagster definitions.\
- **resource\_config\_by\_key** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Specifies config for provided resources. The key in this dictionary corresponds to configuring the same key in the resource\_defs dictionary.\
\
Examples:\
\
```codeBlockLines_e6Vv\
from dagster import asset, resource, with_resources\
\
@resource(config_schema={"bar": str})\
def foo_resource():\
    ...\
\
@asset(required_resource_keys={"foo"})\
def asset1(context):\
    foo = context.resources.foo\
    ...\
\
@asset(required_resource_keys={"foo"})\
def asset2(context):\
    foo = context.resources.foo\
    ...\
\
asset1_with_foo, asset2_with_foo = with_resources(\
    [asset1, asset2],\
    resource_defs={\
        "foo": foo_resource\
    },\
    resource_config_by_key={\
        "foo": {\
            "config": {"bar": ...}\
        }\
    }\
)\
\
```\
\
## Utilities [​](https://docs.dagster.io/api/dagster/resources\#utilities "Direct link to Utilities")\
\
`class` dagster.EnvVar  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_config/field_utils.py#L523)\
\
Class used to represent an environment variable in the Dagster config system.\
\
This class is intended to be used to populate config fields or resources.\
The environment variable will be resolved to a string value when the config is\
loaded.\
\
To access the value of the environment variable, use the get\_value method.\
\
## Legacy resource system [​](https://docs.dagster.io/api/dagster/resources\#legacy-resource-system "Direct link to Legacy resource system")\
\
The following classes are used as part of the [legacy resource system](https://legacy-docs.dagster.io/concepts/resources-legacy).\
\
@dagster.resource  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/resource_definition.py#L375)\
\
Define a resource.\
\
The decorated function should accept an [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) and return an instance of\
the resource. This function will become the `resource_fn` of an underlying\
[`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition).\
\
If the decorated function yields once rather than returning (in the manner of functions\
decorable with `@contextlib.contextmanager`) then\
the body of the function after the yield will be run after execution resolves, allowing users\
to write their own teardown/cleanup logic.\
\
Parameters:\
\
- **config\_schema** ( _Optional_ _\[_ [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema) _\]_) – The schema for the config. Configuration data available in init\_context.resource\_config. If not set, Dagster will accept any config provided.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A human-readable description of the resource.\
- **version** ( _Optional_ _\[_ _str_ _\]_) – beta (Beta) The version of a resource function. Two wrapped resource functions should only have the same version if they produce the same resource definition when provided with the same inputs.\
- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Keys for the resources required by this resource.\
\
- [Pythonic resource system](https://docs.dagster.io/api/dagster/resources#pythonic-resource-system)\
- [Utilities](https://docs.dagster.io/api/dagster/resources#utilities)\
- [Legacy resource system](https://docs.dagster.io/api/dagster/resources#legacy-resource-system)\
\
