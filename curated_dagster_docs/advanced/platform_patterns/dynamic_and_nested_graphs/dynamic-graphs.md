
On this page

Assets vs ops

If you are just getting started with Dagster, we strongly recommend you use [assets](https://docs.dagster.io/guides/build/assets/) rather than ops to build your data pipelines. The ops documentation is for Dagster users who need to manage existing ops, or who have complex use cases.

The ability for portions of a [graph](https://docs.dagster.io/guides/build/ops/graphs) to be duplicated at runtime.

## Relevant APIs [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`DynamicOut`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOut) | Declare that an op will return dynamic outputs |
| [`DynamicOutput`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOutput) | The object that an op will yield repeatedly, each containing a value and a unique mapping\_key |

## Overview [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#overview "Direct link to Overview")

The basic unit of computation in Dagster is the [op](https://docs.dagster.io/guides/build/ops/). In certain cases it is desirable to run the same op multiple times on different pieces of similar data.

Dynamic outputs are the tool Dagster provides to allow resolving the pieces of data at runtime and having downstream copies of the ops created for each piece.

## Using dynamic outputs [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#using-dynamic-outputs "Direct link to Using dynamic outputs")

### A static job [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#a-static-job "Direct link to A static job")

Here we start with a contrived example of a job containing a single expensive op:

```codeBlockLines_e6Vv
@op
def data_processing():
    large_data = load_big_data()
    interesting_result = expensive_processing(large_data)
    return analyze(interesting_result)

@job
def naive():
    data_processing()

```

While, the implementation of `expensive_processing` can internally do something to parallelize the compute, if anything goes wrong with any part we have to restart the whole computation.

### A dynamic job [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#a-dynamic-job "Direct link to A dynamic job")

With this motivation we will break up the computation using Dynamic Outputs. First we will define our new op that will use dynamic outputs. First we use [`DynamicOut`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOut) to declare that this op will return dynamic outputs. Then in the function we `yield` a number of [`DynamicOutput`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOutput) objects that each contain a value and a unique `mapping_key`.

```codeBlockLines_e6Vv
@op(out=DynamicOut())
def load_pieces():
    large_data = load_big_data()
    for idx, piece in large_data.chunk():
        yield DynamicOutput(piece, mapping_key=idx)

```

Then after creating ops for our downstream operations, we can put them all together in a job.

```codeBlockLines_e6Vv
@job
def dynamic_graph():
    pieces = load_pieces()
    results = pieces.map(compute_piece)
    merge_and_analyze(results.collect())

```

Within our `@job` decorated composition function, the object representing the dynamic output can not be passed directly to another op. Either `map` or `collect` must be invoked on it.

`map` takes a `Callable` which receives a single argument. This callable is evaluated once, and any invoked op that is passed the input argument will establish dependencies. The ops downstream of a dynamic output will be cloned for each dynamic output, and identified using the associated `mapping_key`. The return value from the callable is captured and wrapped in an object that allows for subsequent `map` or `collect` calls.

`collect` creates a fan-in dependency over all the dynamic copies. The dependent op will receive a list containing all the values.

Below is a full example that extends the above concepts and uses a mocked large dataset for simulating chunked processing:

```codeBlockLines_e6Vv
import dagster as dg
import numpy as np

def load_big_data():
    # Mock a large dataset by creating a large array of numbers
    large_data = np.arange(100)

    # Define a simple chunk method to simulate chunking the data
    class LargeData:
        def __init__(self, data):
            self.data = data

        def chunk(self, chunk_size=2):
            for i in range(0, len(self.data), chunk_size):
                yield i // chunk_size, self.data[i : i + chunk_size]

    return LargeData(large_data)

@dg.op
def compute_piece(context: dg.OpExecutionContext, piece):
    context.log.info(f"Computing piece: {piece}")
    computed_piece = piece * 2
    context.log.info(f"Computed piece: {computed_piece}")
    return computed_piece

@dg.op
def merge_and_analyze(context: dg.OpExecutionContext, pieces):
    total = sum(pieces)
    context.log.info(f"Total sum of pieces: {total}")
    return total

@dg.op(out=dg.DynamicOut())
def load_pieces():
    large_data = load_big_data()
    for idx, piece in large_data.chunk():
        yield dg.DynamicOutput(piece, mapping_key=str(idx))

@dg.job
def dynamic_graph():
    pieces = load_pieces()
    results = pieces.map(compute_piece)
    merge_and_analyze(results.collect())

defs = dg.Definitions(jobs=[dynamic_graph])

```

## Advanced mapping examples [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#advanced-mapping-examples "Direct link to Advanced mapping examples")

### Returning dynamic outputs [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#returning-dynamic-outputs "Direct link to Returning dynamic outputs")

In addition to yielding, [`DynamicOutput`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOutput) objects can also be returned as part of a list.

```codeBlockLines_e6Vv
from dagster import DynamicOut, DynamicOutput, op

@op(out=DynamicOut())
def return_dynamic() -> list[DynamicOutput[str]]:
    outputs = []
    for idx, page_key in get_pages():
        outputs.append(DynamicOutput(page_key, mapping_key=idx))
    return outputs

```

[`DynamicOutput`](https://docs.dagster.io/api/dagster/dynamic#dagster.DynamicOutput) can be used as a generic type annotation describing the expected type of the output.

### Chaining [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#chaining "Direct link to Chaining")

The following two examples are equivalent ways to establish a sequence of ops that occur for each dynamic output.

```codeBlockLines_e6Vv
@job
def chained():
    results = dynamic_values().map(echo).map(echo).map(echo)
    process(results.collect())

@job
def chained_alt():
    def _for_each(val):
        a = echo(val)
        b = echo(a)
        return echo(b)

    results = dynamic_values().map(_for_each)
    process(results.collect())

```

### Additional arguments [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#additional-arguments "Direct link to Additional arguments")

A lambda or scoped function can be used to pass non-dynamic outputs along side dynamic ones in `map` downstream.

```codeBlockLines_e6Vv
@job
def other_arg():
    non_dynamic = one()
    dynamic_values().map(lambda val: add(val, non_dynamic))

```

### Multiple outputs [​](https://docs.dagster.io/guides/build/ops/dynamic-graphs\#multiple-outputs "Direct link to Multiple outputs")

Multiple outputs are returned via a `namedtuple`, where each entry can be used via `map` or `collect`.

```codeBlockLines_e6Vv
@op(
    out={
        "values": DynamicOut(),
        "negatives": DynamicOut(),
    },
)
def multiple_dynamic_values():
    for i in range(2):
        yield DynamicOutput(i, output_name="values", mapping_key=f"num_{i}")
        yield DynamicOutput(-i, output_name="negatives", mapping_key=f"neg_{i}")

@job
def multiple():
    # can unpack on assignment (order based)
    values, negatives = multiple_dynamic_values()
    process(values.collect())
    process(negatives.map(echo).collect())  # can use map or collect as usual

    # or access by name
    outs = multiple_dynamic_values()
    process(outs.values.collect())
    process(outs.negatives.map(echo).collect())

```

- [Relevant APIs](https://docs.dagster.io/guides/build/ops/dynamic-graphs#relevant-apis)
- [Overview](https://docs.dagster.io/guides/build/ops/dynamic-graphs#overview)
- [Using dynamic outputs](https://docs.dagster.io/guides/build/ops/dynamic-graphs#using-dynamic-outputs)
  - [A static job](https://docs.dagster.io/guides/build/ops/dynamic-graphs#a-static-job)
  - [A dynamic job](https://docs.dagster.io/guides/build/ops/dynamic-graphs#a-dynamic-job)
- [Advanced mapping examples](https://docs.dagster.io/guides/build/ops/dynamic-graphs#advanced-mapping-examples)
  - [Returning dynamic outputs](https://docs.dagster.io/guides/build/ops/dynamic-graphs#returning-dynamic-outputs)
  - [Chaining](https://docs.dagster.io/guides/build/ops/dynamic-graphs#chaining)
  - [Additional arguments](https://docs.dagster.io/guides/build/ops/dynamic-graphs#additional-arguments)
  - [Multiple outputs](https://docs.dagster.io/guides/build/ops/dynamic-graphs#multiple-outputs)
