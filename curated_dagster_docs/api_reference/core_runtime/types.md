
On this page

Dagster includes facilities for typing the input and output values of ops (“runtime” types).

## Built-in types [​](https://docs.dagster.io/api/dagster/types\#built-in-types "Direct link to Built-in types")

dagster.Nothing

Use this type only for inputs and outputs, in order to establish an execution dependency without
communicating a value. Inputs of this type will not be passed to the op compute function, so
it is necessary to use the explicit [`In`](https://docs.dagster.io/api/dagster/ops#dagster.In) API to define them rather than
the Python 3 type hint syntax.

All values are considered to be instances of `Nothing`.

**Examples:**

```codeBlockLines_e6Vv
@op
def wait(_) -> Nothing:
    time.sleep(1)
    return

@op(
    ins={"ready": In(dagster_type=Nothing)},
)
def done(_) -> str:
    return 'done'

@job
def nothing_job():
    done(wait())

# Any value will pass the type check for Nothing
@op
def wait_int(_) -> Int:
    time.sleep(1)
    return 1

@job
def nothing_int_job():
    done(wait_int())

```

## Making New Types [​](https://docs.dagster.io/api/dagster/types\#making-new-types "Direct link to Making New Types")

`class` dagster.DagsterType  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/dagster_type.py#L72)

Define a type in dagster. These can be used in the inputs and outputs of ops.

Parameters:

- **type\_check\_fn** ( _Callable_ _\[_ _\[_ [_TypeCheckContext_](https://docs.dagster.io/api/dagster/execution#dagster.TypeCheckContext) _,_ _Any_ _\]_ _,_ _\[_ _Union_ _\[_ _bool_ _,_ [_TypeCheck_](https://docs.dagster.io/api/dagster/ops#dagster.TypeCheck) _\]_ _\]_ _\]_) – The function that defines the type check. It takes the value flowing through the input or output of the op. If it passes, return either `True` or a [`TypeCheck`](https://docs.dagster.io/api/dagster/ops#dagster.TypeCheck) with `success` set to `True`. If it fails, return either `False` or a [`TypeCheck`](https://docs.dagster.io/api/dagster/ops#dagster.TypeCheck) with `success` set to `False`. The first argument must be named `context` (or, if unused, `_`, `_context`, or `context_`). Use `required_resource_keys` for access to resources.

- **key** ( _Optional_ _\[_ _str_ _\]_) –

The unique key to identify types programmatically. The key property always has a value. If you omit key to the argument to the init function, it instead receives the value of `name`. If neither `key` nor `name` is provided, a `CheckError` is thrown.

In the case of a generic type such as `List` or `Optional`, this is generated programmatically based on the type parameters.

- **name** ( _Optional_ _\[_ _str_ _\]_) – A unique name given by a user. If `key` is `None`, `key` becomes this value. Name is not given in a case where the user does not specify a unique name for this type, such as a generic class.

- **description** ( _Optional_ _\[_ _str_ _\]_) – A markdown-formatted string, displayed in tooling.

- **loader** ( _Optional_ _\[_ [_DagsterTypeLoader_](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) _\]_) – An instance of a class that inherits from [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) and can map config data to a value of this type. Specify this argument if you will need to shim values of this type using the config machinery. As a rule, you should use the [`@dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader) decorator to construct these arguments.

- **required\_resource\_keys** ( _Optional_ _\[_ _Set_ _\[_ _str_ _\]_ _\]_) – Resource keys required by the `type_check_fn`.

- **is\_builtin** ( _bool_) – Defaults to False. This is used by tools to display or filter built-in types (such as `String`, `Int`) to visually distinguish them from user-defined types. Meant for internal use.

- **kind** ( _DagsterTypeKind_) – Defaults to None. This is used to determine the kind of runtime type for InputDefinition and OutputDefinition type checking.

- **typing\_type** – Defaults to None. A valid python typing type (e.g. Optional\[List\[int\]\]) for the value contained within the DagsterType. Meant for internal use.


type\_check  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/dagster_type.py#L171)

Type check the value against the type.

Parameters:

- **context** ( [_TypeCheckContext_](https://docs.dagster.io/api/dagster/execution#dagster.TypeCheckContext)) – The context of the type check.
- **value** ( _Any_) – The value to check.

Returns: The result of the type check.Return type: [TypeCheck](https://docs.dagster.io/api/dagster/ops#dagster.TypeCheck)

`property` description

Description of the type, or None if not provided.

Type: Optional\[str\]

`property` display\_name

Either the name or key (if name is None) of the type, overridden in many subclasses.

`property` has\_unique\_name

Whether the type has a unique name.

Type: bool

`property` loader

Loader for this type, if any.

Type: Optional\[ [DagsterTypeLoader](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader)\]

`property` required\_resource\_keys

Set of resource keys required by the type check function.

Type: AbstractSet\[str\]

`property` typing\_type

The python typing type for this type.

Type: Any

`property` unique\_name

The unique name of this type. Can be None if the type is not unique, such as container types.

dagster.PythonObjectDagsterType  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/dagster_type.py#L525)

Define a type in dagster whose typecheck is an isinstance check.

Specifically, the type can either be a single python type (e.g. int),
or a tuple of types (e.g. (int, float)) which is treated as a union.

Examples:

```codeBlockLines_e6Vv
ntype = PythonObjectDagsterType(python_type=int)
assert ntype.name == 'int'
assert_success(ntype, 1)
assert_failure(ntype, 'a')

```

```codeBlockLines_e6Vv
ntype = PythonObjectDagsterType(python_type=(int, float))
assert ntype.name == 'Union[int, float]'
assert_success(ntype, 1)
assert_success(ntype, 1.5)
assert_failure(ntype, 'a')

```

Parameters:

- **python\_type** ( _Union_ _\[_ _Type_ _,_ _Tuple_ _\[_ _Type_ _,_ _..._ _\]_) – The dagster typecheck function calls instanceof on this type.\
- **name** ( _Optional_ _\[_ _str_ _\]_) – Name the type. Defaults to the name of `python_type`.\
- **key** ( _Optional_ _\[_ _str_ _\]_) – Key of the type. Defaults to name.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A markdown-formatted string, displayed in tooling.\
- **loader** ( _Optional_ _\[_ [_DagsterTypeLoader_](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) _\]_) – An instance of a class that inherits from [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) and can map config data to a value of this type. Specify this argument if you will need to shim values of this type using the config machinery. As a rule, you should use the [`@dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader) decorator to construct these arguments.\
\
dagster.dagster\_type\_loader  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/config_schema.py#L80)\
\
Create an dagster type loader that maps config data to a runtime value.\
\
The decorated function should take the execution context and parsed config value and return the\
appropriate runtime value.\
\
Parameters: **config\_schema** ( [_ConfigSchema_](https://docs.dagster.io/api/dagster/config#dagster.ConfigSchema)) – The schema for the config that’s passed to the decorated\
function.\
Examples:\
\
```codeBlockLines_e6Vv\
@dagster_type_loader(Permissive())\
def load_dict(_context, value):\
    return value\
\
```\
\
`class` dagster.DagsterTypeLoader  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/config_schema.py#L19)\
\
Dagster type loaders are used to load unconnected inputs of the dagster type they are attached\
to.\
\
The recommended way to define a type loader is with the\
[`@dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader) decorator.\
\
`class` dagster.DagsterTypeLoaderContext  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/context/system.py#L1346)\
\
The context object provided to a [`@dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader)-decorated function during execution.\
\
Users should not construct this object directly.\
\
`property` job\_def\
\
The underlying job definition being executed.\
\
`property` op\_def\
\
The op for which type loading is occurring.\
\
`property` resources\
\
The resources available to the type loader, specified by the required\_resource\_keys argument of the decorator.\
\
dagster.usable\_as\_dagster\_type  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/decorator.py#L29)\
\
Decorate a Python class to make it usable as a Dagster Type.\
\
This is intended to make it straightforward to annotate existing business logic classes to\
make them dagster types whose typecheck is an isinstance check against that python class.\
\
Parameters:\
\
- **python\_type** ( _cls_) – The python type to make usable as python type.\
- **name** ( _Optional_ _\[_ _str_ _\]_) – Name of the new Dagster type. If `None`, the name ( `__name__`) of the `python_type` will be used.\
- **description** ( _Optional_ _\[_ _str_ _\]_) – A user-readable description of the type.\
- **loader** ( _Optional_ _\[_ [_DagsterTypeLoader_](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) _\]_) – An instance of a class that inherits from [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) and can map config data to a value of this type. Specify this argument if you will need to shim values of this type using the config machinery. As a rule, you should use the [`@dagster_type_loader`](https://docs.dagster.io/api/dagster/types#dagster.dagster_type_loader) decorator to construct these arguments.\
\
Examples:\
\
```codeBlockLines_e6Vv\
# dagster_aws.s3.file_manager.S3FileHandle\
@usable_as_dagster_type\
class S3FileHandle(FileHandle):\
    def __init__(self, s3_bucket, s3_key):\
        self._s3_bucket = check.str_param(s3_bucket, 's3_bucket')\
        self._s3_key = check.str_param(s3_key, 's3_key')\
\
    @property\
    def s3_bucket(self):\
        return self._s3_bucket\
\
    @property\
    def s3_key(self):\
        return self._s3_key\
\
    @property\
    def path_desc(self):\
        return self.s3_path\
\
    @property\
    def s3_path(self):\
        return 's3://{bucket}/{key}'.format(bucket=self.s3_bucket, key=self.s3_key)\
\
```\
\
dagster.make\_python\_type\_usable\_as\_dagster\_type  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/types/dagster_type.py#L812)\
\
Take any existing python type and map it to a dagster type (generally created with\
[`DagsterType`](https://docs.dagster.io/api/dagster/types#dagster.DagsterType)) This can only be called once\
on a given python type.\
\
### Testing Types [​](https://docs.dagster.io/api/dagster/types\#testing-types "Direct link to Testing Types")\
\
dagster.check\_dagster\_type  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_utils/dagster_type.py#L14)\
\
Test a custom Dagster type.\
\
Parameters:\
\
- **dagster\_type** ( _Any_) – The Dagster type to test. Should be one of the [built-in types](https://docs.dagster.io/api/dagster/types#builtin) `built-in types`, a dagster type explicitly constructed with `as_dagster_type()`, `@usable_as_dagster_type`, or [`PythonObjectDagsterType()`](https://docs.dagster.io/api/dagster/types#dagster.PythonObjectDagsterType), or a Python type.\
- **value** ( _Any_) – The runtime value to test.\
\
Returns: The result of the type check.Return type: [TypeCheck](https://docs.dagster.io/api/dagster/ops#dagster.TypeCheck)\
Examples:\
\
```codeBlockLines_e6Vv\
assert check_dagster_type(Dict[Any, Any], {'foo': 'bar'}).success\
\
```\
\
- [Built-in types](https://docs.dagster.io/api/dagster/types#built-in-types)\
- [Making New Types](https://docs.dagster.io/api/dagster/types#making-new-types)\
  - [Testing Types](https://docs.dagster.io/api/dagster/types#testing-types)\
\
