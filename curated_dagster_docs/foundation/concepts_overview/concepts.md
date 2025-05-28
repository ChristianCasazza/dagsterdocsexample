
On this page

Dagster provides a variety of abstractions for building and orchestrating data pipelines. These concepts enable a modular, declarative approach to data engineering, making it easier to manage dependencies, monitor execution, and ensure data quality.

Component

Definitions

AssetCheck

Asset

Config

Code Location

Graph

IO Manager

Job

Op

Partition

Resource

Schedule

Sensor

Type

### Asset [​](https://docs.dagster.io/getting-started/concepts\#asset "Direct link to Asset")

Config

Partition

Job

Schedule

Sensor

IOManager

Resource

AssetCheck

Definitions

Asset

An [`asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) represents a logical unit of data such as a table, dataset, or machine learning model. Assets can have dependencies on other assets, forming the data lineage for your pipelines. As the core abstraction in Dagster, assets can interact with many other Dagster concepts to facilitate certain tasks.

| Concept | Relationship |
| --- | --- |
| [asset check](https://docs.dagster.io/getting-started/concepts#asset-check) | `asset` may use an `asset check` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `asset` may use a `config` |
| [io manager](https://docs.dagster.io/getting-started/concepts#io-manager) | `asset` may use a `io manager` |
| [partition](https://docs.dagster.io/getting-started/concepts#partition) | `asset` may use a `partition` |
| [resource](https://docs.dagster.io/getting-started/concepts#resource) | `asset` may use a `resource` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `asset` may be used in a `job` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `asset` may be used in a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `asset` may be used in a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `asset` must be set in a `definitions` to be deployed |

### Asset Check [​](https://docs.dagster.io/getting-started/concepts\#asset-check "Direct link to Asset Check")

Asset

Asset Check

Definitions

An [`asset_check`](https://docs.dagster.io/api/dagster/asset-checks#dagster.asset_check) is associated with an [`asset`](https://docs.dagster.io/api/dagster/assets#dagster.asset) to ensure it meets certain expectations around data quality, freshness or completeness. Asset checks run when the asset is executed and store metadata about the related run and if all the conditions of the check were met.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `asset check` may be used by an `asset` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `asset check` must be set in a `definitions` to be deployed |

### Code Location [​](https://docs.dagster.io/getting-started/concepts\#code-location "Direct link to Code Location")

Definitions

Code Location

A `code location` is a collection of [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) deployed in a specific environment. A code location determines the Python environment (including the version of Dagster being used as well as any other Python dependencies). A Dagster project can have multiple code locations, helping isolate dependencies.

| Concept | Relationship |
| --- | --- |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `code location` must contain at least one `definitions` |

### Component [​](https://docs.dagster.io/getting-started/concepts\#component "Direct link to Component")

Component

Definitions

A `Component` is an opinionated project layout built around a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). The [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) contains Dagster objects used to accomplish a specific task—such as a standard workflow or an integration. Components are designed to help you quickly bootstrap parts of your Dagster project and serve as templates for repeatable patterns.

| Concept | Relationship |
| --- | --- |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `component` produces a `definitions` |

### Config [​](https://docs.dagster.io/getting-started/concepts\#config "Direct link to Config")

Config

Job

Asset

Schedule

Sensor

A [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig) is a set schema applied to a Dagster object that is input at the time of execution. This allows for parameterization and the reuse of pipelines to serve multiple purposes.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `config` may be used by an `asset` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `config` may be used by a `job` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `config` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `config` may be used by a `sensor` |

### Definitions [​](https://docs.dagster.io/getting-started/concepts\#definitions "Direct link to Definitions")

Job

Schedule

Asset

Sensor

IOManager

Resource

AssetCheck

CodeLocation

Definitions

[`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) is a top-level construct that contains
references to all the objects of a Dagster project, such as
[`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset),
[`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) and
[`ScheduleDefinitions`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition). Only objects included
in the definitions will be deployed and visible within the Dagster UI.

A [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) can also be constructed from a `Component` which is a predefined set of Dagster objects.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `definitions` may contain one or more `assets` |
| [asset check](https://docs.dagster.io/getting-started/concepts#asset-check) | `definitions` may contain one or more `asset checks` |
| [io manager](https://docs.dagster.io/getting-started/concepts#io-manager) | `definitions` may contain one or more `io managers` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `definitions` may contain one or more `jobs` |
| [resource](https://docs.dagster.io/getting-started/concepts#resource) | `definitions` may contain one or more `resources` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `definitions` may contain one or more `schedules` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `definitions` may contain one or more `sensors` |
| [component](https://docs.dagster.io/getting-started/concepts#component) | `definitions` may exist as the output of a `component` |
| [code location](https://docs.dagster.io/getting-started/concepts#code-location) | `definitions` must be deployed in a `code location` |

### Graph [​](https://docs.dagster.io/getting-started/concepts\#graph "Direct link to Graph")

Config

Op

Graph

Job

A [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) connects multiple [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op) together to form a DAG. If you are using [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset), you will not need to use graphs directly.

| Concept | Relationship |
| --- | --- |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `graph` may use a `config` |
| [op](https://docs.dagster.io/getting-started/concepts#op) | `graph` must include one or more `ops` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `graph` must be part of `job` to execute |

### IO Manager [​](https://docs.dagster.io/getting-started/concepts\#io-manager "Direct link to IO Manager")

Asset

IO Manager

Definitions

An [`IOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.IOManager) defines how data is stored and retrieved between the execution of [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) and [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op). This allows for a customizable storage and format at any interaction in a pipeline.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `io manager` may be used by an `asset` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `io manager` must be set in a `definitions` to be deployed |

### Job [​](https://docs.dagster.io/getting-started/concepts\#job "Direct link to Job")

Asset

Graph

Config

Schedule

Sensor

Definitions

Job

A [`job`](https://docs.dagster.io/api/dagster/jobs#dagster.job) is a subset of [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) or the [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) of [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op). Jobs are the main form of execution in Dagster.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `job` may contain a selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `job` may use a `config` |
| [graph](https://docs.dagster.io/getting-started/concepts#graph) | `job` may contain a `graph` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `job` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `job` may be used by a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `job` must be set in a `definitions` to be deployed |

### Op [​](https://docs.dagster.io/getting-started/concepts\#op "Direct link to Op")

Type

Op

Graph

An [`op`](https://docs.dagster.io/api/dagster/ops#dagster.op) is a computational unit of work. Ops are arranged into a [`GraphDefinition`](https://docs.dagster.io/api/dagster/graphs#dagster.GraphDefinition) to dictate their order. Ops have largely been replaced by [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset).

| Concept | Relationship |
| --- | --- |
| [type](https://docs.dagster.io/getting-started/concepts#type) | `op` may use a `type` |
| [graph](https://docs.dagster.io/getting-started/concepts#graph) | `op` must be contained in `graph` to execute |

### Partition [​](https://docs.dagster.io/getting-started/concepts\#partition "Direct link to Partition")

Partition

Asset

A [`PartitionsDefinition`](https://docs.dagster.io/api/dagster/partitions#dagster.PartitionsDefinition) represents a logical slice of a dataset or computation mapped to a certain segments (such as increments of time). Partitions enable incremental processing, making workflows more efficient by only running on relevant subsets of data.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `partition` may be used by an `asset` |

### Resource [​](https://docs.dagster.io/getting-started/concepts\#resource "Direct link to Resource")

Resource

Asset

Schedule

Sensor

Definitions

A [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) is a configurable external dependency. These can be databases, APIs, or anything outside of Dagster.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `resource` may be used by an `asset` |
| [schedule](https://docs.dagster.io/getting-started/concepts#schedule) | `resource` may be used by a `schedule` |
| [sensor](https://docs.dagster.io/getting-started/concepts#sensor) | `resource` may be used by a `sensor` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `resource` must be set in a `definitions` to be deployed |

### Type [​](https://docs.dagster.io/getting-started/concepts\#type "Direct link to Type")

Type

Op

A `type` is a way to define and validate the data passed between [`ops`](https://docs.dagster.io/api/dagster/ops#dagster.op).

| Concept | Relationship |
| --- | --- |
| [op](https://docs.dagster.io/getting-started/concepts#op) | `type` may be used by an `op` |

### Schedule [​](https://docs.dagster.io/getting-started/concepts\#schedule "Direct link to Schedule")

Asset

Config

Definitions

Job

Schedule

A [`ScheduleDefinition`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.ScheduleDefinition) is a way to automate [`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) to occur on a specified interval. In the cases that a job or asset is parameterized, the schedule can also be set with a run configuration ( [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig)) to match.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `schedule` may include a `job` or selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `schedule` may include a `config` if the `job` or `assets` include a `config` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `schedule` may include a `job` or selection of `assets` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `schedule` must be set in a `definitions` to be deployed |

### Sensor [​](https://docs.dagster.io/getting-started/concepts\#sensor "Direct link to Sensor")

Asset

Config

Definitions

Job

Sensor

A `sensor` is a way to trigger [`jobs`](https://docs.dagster.io/api/dagster/jobs#dagster.job) or [`assets`](https://docs.dagster.io/api/dagster/assets#dagster.asset) when an event occurs, such as a file being uploaded or a push notification. In the cases that a job or asset is parameterized, the sensor can also be set with a run configuration ( [`RunConfig`](https://docs.dagster.io/api/dagster/config#dagster.RunConfig)) to match.

| Concept | Relationship |
| --- | --- |
| [asset](https://docs.dagster.io/getting-started/concepts#asset) | `sensor` may include a `job` or selection of `assets` |
| [config](https://docs.dagster.io/getting-started/concepts#config) | `sensor` may include a `config` if the `job` or `assets` include a `config` |
| [job](https://docs.dagster.io/getting-started/concepts#job) | `sensor` may include a `job` or selection of `assets` |
| [definitions](https://docs.dagster.io/getting-started/concepts#definitions) | `sensor` must be set in a `definitions` to be deployed |

- [Asset](https://docs.dagster.io/getting-started/concepts#asset)
- [Asset Check](https://docs.dagster.io/getting-started/concepts#asset-check)
- [Code Location](https://docs.dagster.io/getting-started/concepts#code-location)
- [Component](https://docs.dagster.io/getting-started/concepts#component)
- [Config](https://docs.dagster.io/getting-started/concepts#config)
- [Definitions](https://docs.dagster.io/getting-started/concepts#definitions)
- [Graph](https://docs.dagster.io/getting-started/concepts#graph)
- [IO Manager](https://docs.dagster.io/getting-started/concepts#io-manager)
- [Job](https://docs.dagster.io/getting-started/concepts#job)
- [Op](https://docs.dagster.io/getting-started/concepts#op)
- [Partition](https://docs.dagster.io/getting-started/concepts#partition)
- [Resource](https://docs.dagster.io/getting-started/concepts#resource)
- [Type](https://docs.dagster.io/getting-started/concepts#type)
- [Schedule](https://docs.dagster.io/getting-started/concepts#schedule)
- [Sensor](https://docs.dagster.io/getting-started/concepts#sensor)
