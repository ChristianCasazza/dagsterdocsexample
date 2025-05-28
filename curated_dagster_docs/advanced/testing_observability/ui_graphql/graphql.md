
On this page

note

The GraphQL API is still evolving and is subject to breaking changes. A large portion of the API is primarily for internal use by the [Dagster webserver](https://docs.dagster.io/guides/operate/webserver).
For any of the queries below, we will be clear about breaking changes in release notes.

Dagster exposes a GraphQL API that allows clients to interact with Dagster programmatically. The API allows users to:

- Query information about Dagster runs, both historical and currently executing
- Retrieve metadata about repositories, jobs, and ops, such as dependency structure and config schemas
- Launch job executions and re-executions, allowing users to trigger executions on custom events

## Using the GraphQL API [​](https://docs.dagster.io/guides/operate/graphql\#using-the-graphql-api "Direct link to Using the GraphQL API")

The GraphQL API is served from the [webserver](https://docs.dagster.io/guides/operate/webserver). To start the server, run the following:

```codeBlockLines_e6Vv
dagster dev

```

The webserver serves the GraphQL endpoint at the `/graphql` endpoint. If you are running the webserver locally on port 3000, you can access the API at [http://localhost:3000/graphql](http://localhost:3000/graphql).

### Using the GraphQL playground [​](https://docs.dagster.io/guides/operate/graphql\#using-the-graphql-playground "Direct link to Using the GraphQL playground")

You can access the GraphQL Playground by navigating to the `/graphql` route in your browser. The GraphQL playground contains the full GraphQL schema and an interactive playground to write and test queries and mutations:

![GraphQL playground](https://docs.dagster.io/assets/images/playground-1ecf160c5e11dc2bcb805ff4b56bee51.png)

### Exploring the GraphQL schema and documentation [​](https://docs.dagster.io/guides/operate/graphql\#exploring-the-graphql-schema-and-documentation "Direct link to Exploring the GraphQL schema and documentation")

Clicking on the **Docs** tab on the right edge of the playground opens up interactive documentation for the GraphQL API. The interactive documentation is the best way to explore the API and get information about which fields are available on the queries and mutations:

![GraphQL docs](https://docs.dagster.io/assets/images/docs-6ac04a19cdf0a6efb8bf00a8423b5228.png)

## Python client [​](https://docs.dagster.io/guides/operate/graphql\#python-client "Direct link to Python client")

Dagster also provides a Python client to interface with Dagster's GraphQL API from Python. For more information, see " [Dagster Python GraphQL client](https://docs.dagster.io/guides/operate/graphql/graphql-client)".

## Example queries [​](https://docs.dagster.io/guides/operate/graphql\#example-queries "Direct link to Example queries")

- [Get a list of Dagster runs](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-dagster-runs)
- [Get a list of repositories](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-repositories)
- [Get a list of jobs within a repository](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-jobs-within-a-repository)
- [Launch a run](https://docs.dagster.io/guides/operate/graphql#launch-a-run)
- [Terminate an in-progress run](https://docs.dagster.io/guides/operate/graphql#terminate-an-in-progress-run)

### Get a list of Dagster runs [​](https://docs.dagster.io/guides/operate/graphql\#get-a-list-of-dagster-runs "Direct link to Get a list of Dagster runs")

- Paginate through runs
- Filtering runs

You may eventually accumulate too many runs to return in one query. The `runsOrError` query takes in optional `cursor` and `limit` arguments for pagination:

```codeBlockLines_e6Vv
query PaginatedRunsQuery($cursor: String) {
  runsOrError(
    cursor: $cursor
    limit: 10
  ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
      }
    }
  }
}

```

The `runsOrError` query also takes in an optional filter argument, of type `RunsFilter`. This query allows you to filter runs by:

- run ID
- job name
- tags
- statuses

For example, the following query will return all failed runs:

```codeBlockLines_e6Vv
query FilteredRunsQuery($cursor: String) {
  runsOrError(
    filter: { statuses: [FAILURE] }
    cursor: $cursor
    limit: 10
  ) {
    __typename
    ... on Runs {
      results {
        runId
        jobName
        status
        runConfigYaml
        startTime
        endTime
      }
    }
  }
}

```

### Get a list of repositories [​](https://docs.dagster.io/guides/operate/graphql\#get-a-list-of-repositories "Direct link to Get a list of repositories")

This query returns the names and location names of all the repositories currently loaded:

```codeBlockLines_e6Vv
query RepositoriesQuery {
  repositoriesOrError {
    ... on RepositoryConnection {
      nodes {
        name
        location {
          name
        }
      }
    }
  }
}

```

### Get a list of jobs within a repository [​](https://docs.dagster.io/guides/operate/graphql\#get-a-list-of-jobs-within-a-repository "Direct link to Get a list of jobs within a repository")

Given a repository, this query returns the names of all the jobs in the repository.

This query takes a `selector`, which is of type `RepositorySelector`. A repository selector consists of both the repository location name and repository name.

```codeBlockLines_e6Vv
query JobsQuery(
  $repositoryLocationName: String!
  $repositoryName: String!
) {
  repositoryOrError(
    repositorySelector: {
      repositoryLocationName: $repositoryLocationName
      repositoryName: $repositoryName
    }
  ) {
    ... on Repository {
      jobs {
        name
      }
    }
  }
}

```

### Launch a run [​](https://docs.dagster.io/guides/operate/graphql\#launch-a-run "Direct link to Launch a run")

To launch a run, use the `launchRun` mutation. Here, we define `LaunchRunMutation` to wrap our mutation and pass in the required arguments as query variables. For this query, the required arguments are:

- `selector` \- A dictionary that contains the repository location name, repository name, and job name.
- `runConfigData` \- The run config for the job execution. **Note**: Note that `runConfigData` is of type `RunConfigData`. This type is used when passing in an arbitrary object for run config. This is any-typed in the GraphQL type system, but must conform to the constraints of the config schema for this job. If it doesn't, the mutation returns a `RunConfigValidationInvalid` response.

```codeBlockLines_e6Vv
mutation LaunchRunMutation(
  $repositoryLocationName: String!
  $repositoryName: String!
  $jobName: String!
  $runConfigData: RunConfigData!
) {
  launchRun(
    executionParams: {
      selector: {
        repositoryLocationName: $repositoryLocationName
        repositoryName: $repositoryName
        jobName: $jobName
      }
      runConfigData: $runConfigData
    }
  ) {
    __typename
    ... on LaunchRunSuccess {
      run {
        runId
      }
    }
    ... on RunConfigValidationInvalid {
      errors {
        message
        reason
      }
    }
    ... on PythonError {
      message
    }
  }
}

```

### Terminate an in-progress run [​](https://docs.dagster.io/guides/operate/graphql\#terminate-an-in-progress-run "Direct link to Terminate an in-progress run")

If you want to stop execution of an in-progress run, use the `terminateRun` mutation. The only required argument for this mutation is the ID of the run.

```codeBlockLines_e6Vv
mutation TerminateRun($runId: String!) {
  terminateRun(runId: $runId){
    __typename
    ... on TerminateRunSuccess{
      run {
        runId
      }
    }
    ... on TerminateRunFailure {
      message
    }
    ... on RunNotFoundError {
      runId
    }
    ... on PythonError {
      message
      stack
    }
  }
}

```

- [Using the GraphQL API](https://docs.dagster.io/guides/operate/graphql#using-the-graphql-api)
  - [Using the GraphQL playground](https://docs.dagster.io/guides/operate/graphql#using-the-graphql-playground)
  - [Exploring the GraphQL schema and documentation](https://docs.dagster.io/guides/operate/graphql#exploring-the-graphql-schema-and-documentation)
- [Python client](https://docs.dagster.io/guides/operate/graphql#python-client)
- [Example queries](https://docs.dagster.io/guides/operate/graphql#example-queries)
  - [Get a list of Dagster runs](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-dagster-runs)
  - [Get a list of repositories](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-repositories)
  - [Get a list of jobs within a repository](https://docs.dagster.io/guides/operate/graphql#get-a-list-of-jobs-within-a-repository)
  - [Launch a run](https://docs.dagster.io/guides/operate/graphql#launch-a-run)
  - [Terminate an in-progress run](https://docs.dagster.io/guides/operate/graphql#terminate-an-in-progress-run)
