
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

```codeBlockLines_e6Vv
@op
def my_op(context: OpExecutionContext, input_string: str):
    context.log.info(f"input string: {input_string}")

@job
def my_job():
    my_op()

```

### Loading a custom Dagster type from config [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#loading-a-custom-dagster-type-from-config "Direct link to Loading a custom Dagster type from config")

When you have an op at the beginning of your job that operates on a Dagster type that you've defined, you can write your own [`DagsterTypeLoader`](https://docs.dagster.io/api/dagster/types#dagster.DagsterTypeLoader) to define how to load that input via run config.

```codeBlockLines_e6Vv
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

```

With this, the input can be specified via config as below:

```codeBlockLines_e6Vv
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

```

## Working with input managers [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#working-with-input-managers "Direct link to Working with input managers")

### Providing an input manager for unconnected inputs [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#providing-an-input-manager-for-unconnected-inputs "Direct link to Providing an input manager for unconnected inputs")

When you have an op at the beginning of a job that operates on data from an external source, you might wish to separate that I/O from your op's business logic, in the same way you would with an I/O manager if the op were loading from an upstream output.

Use the following tabs to learn about how to achieve this in Dagster.

- Option 1: Use the input\_manager decorator
- Option 2: Use a class to implement the InputManager interface

In this example, we wrote a function to load the input and decorated it with [`@dg.input_manager`](https://docs.dagster.io/api/dagster/io-managers#dagster.input_manager):

```codeBlockLines_e6Vv

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

```

In this example, we defined a class that implements the [`InputManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputManager) interface:

```codeBlockLines_e6Vv

class Table1InputManager(dg.InputManager):
    def load_input(self, context: dg.InputContext):
        return read_dataframe_from_table(name="table_1")

@dg.input_manager
def table_1_manager():
    return Table1InputManager()

@dg.job(resource_defs={"load_input_manager": table_1_manager})
def load_table_job():
    my_op()

```

To use `Table1InputManager` to store outputs or override the `load_input` method of an I/O manager used elsewhere in the job, another option is to implement an instance of [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager):

```codeBlockLines_e6Vv
# in this example, TableIOManager is defined elsewhere and we just want to override load_input
class Table1IOManager(TableIOManager):
    def load_input(self, context: dg.InputContext):
        return read_dataframe_from_table(name="table_1")

@dg.job(resource_defs={"load_input_manager": Table1IOManager()})
def io_load_table_job():
    my_op()

```

In any of the examples in Option 1 or Option 2, setting the `input_manager_key` on an `In` controls how that input is loaded.

### Providing per-input config to input managers [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#providing-per-input-config-to-input-managers "Direct link to Providing per-input config to input managers")

When launching a run, you might want to parameterize how particular inputs are loaded.

To accomplish this, you can define an `input_config_schema` on the I/O manager or input manager definition. The `load_input` function can access this config when storing or loading data, via the [`InputContext`](https://docs.dagster.io/api/dagster/io-managers#dagster.InputContext):

```codeBlockLines_e6Vv

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

```

Then, when executing a job, you can pass in this per-input config:

```codeBlockLines_e6Vv

load_table_job.execute_in_process(
    run_config={"ops": {"my_op": {"inputs": {"dataframe": {"table": "table_1"}}}}},
)

```

### Using input managers with subselection [​](https://docs.dagster.io/guides/build/jobs/unconnected-inputs\#using-input-managers-with-subselection "Direct link to Using input managers with subselection")

You might want to execute a subset of ops in your job and control how the inputs of those ops are loaded. Custom input managers also help in these situations, because the inputs at the beginning of the subset become unconnected inputs.

For example, you might have `op1` that normally produces a table that `op2` consumes. To debug `op2`, you might want to run it on a different table than the one normally produced by `op1`.

To accomplish this, you can set up the `input_manager_key` on `op2`'s `In` to point to an input manager with the desired loading behavior. As in the previous example, setting the `input_manager_key` on an `In` controls how that input is loaded and you can write custom loading logic.

```codeBlockLines_e6Vv

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

```

So far, this is set up so that `op2` always loads `table_1` even if you execute the full job. This would let you debug `op2`, but if you want to write this so that `op2` only loads `table_1` when no input is provided from an upstream op, you can rewrite the input manager as a subclass of the IO manager used for the rest of the job as follows:

```codeBlockLines_e6Vv

class MyNewInputLoader(MyIOManager):
    def load_input(self, context: dg.InputContext):
        if context.upstream_output is None:
            # load input from table since there is no upstream output
            return read_dataframe_from_table(name="table_1")
        else:
            return super().load_input(context)

```

Now, when running the full job, `op2`'s input will be loaded using the IO manager on the output of `op1`. When running the job subset, `op2`'s input has no upstream output, so `table_1` will be loaded.

```codeBlockLines_e6Vv
my_subselection_job.execute_in_process(
    op_selection=["op2"],
)

```

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
