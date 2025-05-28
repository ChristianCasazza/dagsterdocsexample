
On this page

Dagster resources are objects used by Dagster assets and ops that provide access to external systems, databases, or services. For example, a simple ETL (Extract Transform Load) pipeline fetches data from an API, ingests it into a database, and updates a dashboard. External tools and services this pipeline uses could be:

- The API the data is fetched from
- The AWS S3 bucket where the API's response is stored
- The Snowflake/Databricks/BigQuery account the data is ingested into
- The BI tool the dashboard was made in

Using Dagster resources, you can standardize connections and integrations to these tools across Dagster definitions like [asset definitions](https://docs.dagster.io/guides/build/assets), [schedules](https://docs.dagster.io/guides/automate/schedules), [sensors](https://docs.dagster.io/guides/automate/sensors), and [jobs](https://docs.dagster.io/guides/build/jobs/).

Resources allow you to:

- **Plug in different implementations in different environments** \- If you have a heavy external dependency that you want to use in production but avoid using in testing, you can accomplish this by providing different resources in each environment.
- **Surface configuration in the Dagster UI** \- Resources and their configuration are surfaced in the UI, making it easy to see where resources are used and how they're configured.
- **Share configuration across multiple assets or ops** \- Resources are configurable and shared, so configuration can be supplied in one place instead of individually.
- **Share implementations across multiple assets or ops** \- When multiple assets access the same external services, resources provide a standard way to structure your code to share the implementations.

## Relevant APIs [​](https://docs.dagster.io/guides/build/external-resources\#relevant-apis "Direct link to Relevant APIs")

| Name | Description |
| --- | --- |
| [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) | The base class extended to define resources. Under the hood, implements [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). |
| `ResourceParam` | An annotation used to specify that a plain Python object parameter for an asset or op is a resource. |
| [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition) | Class for resource definitions. You almost never want to use initialize this class directly. Instead, you should extend the [`ConfigurableResource`](https://docs.dagster.io/api/dagster/resources#dagster.ConfigurableResource) class which implements [`ResourceDefinition`](https://docs.dagster.io/api/dagster/resources#dagster.ResourceDefinition). |
| [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) | The context object provided to a resource during initialization. This object contains required resources, config, and other run information. |
| [`build_init_resource_context`](https://docs.dagster.io/api/dagster/resources#dagster.build_init_resource_context) | Function for building an [`InitResourceContext`](https://docs.dagster.io/api/dagster/resources#dagster.InitResourceContext) outside of execution, intended to be used when testing a resource. |
| [`build_resources`](https://docs.dagster.io/api/dagster/resources#dagster.build_resources) | Function for initializing a set of resources outside of the context of a job's execution. |
| [`with_resources`](https://docs.dagster.io/api/dagster/resources#dagster.with_resources) | Advanced API for providing resources to a specific set of asset definitions, overriding those provided to [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions). |

- [Relevant APIs](https://docs.dagster.io/guides/build/external-resources#relevant-apis)
