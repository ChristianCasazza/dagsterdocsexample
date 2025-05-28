
On this page

note

This article applies to Dagster Open Source (OSS) deployments. For information on Dagster+, see the [Dagster+ documentation](https://docs.dagster.io/dagster-plus/deployment/management/settings/customizing-agent-settings).

The Dagster instance defines the configuration that Dagster needs for a single deployment - for example, where to store the history of past runs and their associated logs, where to stream the raw logs from op compute functions, and how to launch new runs.

All of the processes and services that make up your Dagster deployment should share a single instance config file, named `dagster.yaml`, so that they can effectively share information.

warning

Some important configuration, like [execution parallelism](https://docs.dagster.io/guides/operate/run-executors), is set on a per-job basis rather than on the instance.

## Default local behavior [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#default-local-behavior "Direct link to Default local behavior")

When a Dagster process like the Dagster webserver or Dagster CLI commands are launched, Dagster tries to load your instance. If the environment variable `DAGSTER_HOME` is set, Dagster looks for an instance config file at `$DAGSTER_HOME/dagster.yaml`. This file contains the configuration settings that make up the instance.

If `DAGSTER_HOME` isn't set, Dagster tools will use a temporary directory for storage that is cleaned up when the process exits. This can be useful when using Dagster for temporary local development or testing, when you don't care about the results being persisted.

If `DAGSTER_HOME` is set but `dagster.yaml` isn't present or is empty, Dagster will persist data on the local filesystem, structured like the following:

```codeBlockLines_e6Vv
$DAGSTER_HOME
├── dagster.yaml
├── history
│   ├── runs
│   │   ├── 00636713-98a9-461c-a9ac-d049407059cd.db
│   │   └── ...
│   └── runs.db
└── storage
    ├── 00636713-98a9-461c-a9ac-d049407059cd
    │   └── compute_logs
    │       └── ...
    └── ...

```

Here's a breakdown of the files and directories that are generated:

| File or directory | Description |
| --- | --- |
| history/ | A directory containing historical information for runs. |
| history/runs.db | SQLite database file that contains information about runs. |
| history/\[run\_id\].db | SQLite database file that contains per-run event logs. |
| storage/ | A directory of subdirectories, one for each run. |
| storage/\[run\_id\]/compute\_logs | A directory specific to the run that contains the `stdout` and `stderr` logs from the execution of the compute functions of each op. |

## Configuration reference [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#configuration-reference "Direct link to Configuration reference")

In persistent Dagster deployments, you'll typically want to configure many of the components on the instance. For example, you may want to use a Postgres instance to store runs and the corresponding event logs, but stream compute logs to an Amazon S3 bucket.

To do this, provide a `$DAGSTER_HOME/dagster.yaml` file, which the webserver and all other Dagster tools will look for on startup. In this file, you can configure different aspects of your Dagster instance, including:

| Name | Key | Description |
| --- | --- | --- |
| Dagster storage | `storage` | Controls how job and asset history is persisted. This includes run, event log, and schedule/sensor tick metadata, as well as other useful data. |
| Run launcher | `run_launcher` | Determines where runs are executed. |
| Run coordinator | `run_coordinator` | Determines the policy used to set prioritization rules and concurrency limits for runs. |
| Compute log storage | `compute_logs` | Controls the capture and persistence of raw stdout and stderr ext logs. |
| Local artifact storage | `local_artifact_storage` | Configures storage for artifacts that require a local disk or when using the filesystem I/O manager ( ). |
| Telemetry | `telemetry` | Used to opt in/out of Dagster collecting anonymized usage statistics. |
| gRPC servers | `code_servers` | Configures how Dagster loads the code in a code location. |
| Data retention | `data_retention` | Controls how long Dagster retains certain types of data that have diminishing value over time, such as schedule/sensor tick data. |
| Sensor evaluation | `sensors` | Controls how sensors are evaluated. |
| Schedule evaluation | `schedules` | Controls how schedules are evaluated. |
| Auto-materialize | `auto_materialize` | Controls how assets are auto-materialized. |
| Backfills | `backfills` | Controls how backfills are processed. |

note

Environment variables in YAML configuration are supported by using an `env:` key instead of a literal string value. Sample configurations in this reference include examples using environment variables.

### Dagster storage [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#dagster-storage "Direct link to Dagster storage")

The `storage` key allows you to configure how job and asset history is persisted. This includes metadata on runs, event logs, schedule/sensor ticks, and other useful data.

Refer to the following tabs for available options and sample configuration.

- Sqlite storage (default)
- Postgres storage
- MySQL storage

**SQLite storage (default)**

To use a SQLite database for storage, configure `storage.sqlite` in `dagster.yaml`:

```codeBlockLines_e6Vv

# there are two ways to set storage to SqliteStorage

# this config manually sets the directory (`base_dir`) for Sqlite to store data in:
storage:
  sqlite:
    base_dir: /path/to/dir

# and this config grabs the directory from an environment variable
storage:
  sqlite:
    base_dir:
      env: SQLITE_STORAGE_BASE_DIR

```

**Postgres storage**

note

To use Postgres storage, you'll need to install the [dagster-postgres](https://docs.dagster.io/api/libraries/dagster-postgres) library.

To use a [PostgreSQL database](https://docs.dagster.io/api/libraries/dagster-postgres) for storage, configure `storage.postgres` in `dagster.yaml`:

```codeBlockLines_e6Vv

# Postgres storage can be set using either credentials or a connection string.  This requires that
# the `dagster-postgres` library be installed and a database configured with UTC timezone.

# this config manually sets the Postgres credentials
storage:
  postgres:
    postgres_db:
      username: { DAGSTER_PG_USERNAME }
      password: { DAGSTER_PG_PASSWORD }
      hostname: { DAGSTER_PG_HOSTNAME }
      db_name: { DAGSTER_PG_DB }
      port: 5432

# and this config grabs the database credentials from environment variables
storage:
  postgres:
    postgres_db:
      username:
        env: DAGSTER_PG_USERNAME
      password:
        env: DAGSTER_PG_PASSWORD
      hostname:
        env: DAGSTER_PG_HOST
      db_name:
        env: DAGSTER_PG_DB
      port: 5432

# and this config sets the credentials via DB connection string / url:
storage:
  postgres:
    postgres_url: { PG_DB_CONN_STRING }

# This config gets the DB connection string / url via environment variables:
storage:
  postgres:
    postgres_url:
      env: PG_DB_CONN_STRING

```

**MySQL storage**

note

To use MySQL storage, you'll need to install the [dagster-mysql](https://docs.dagster.io/api/libraries/dagster-mysql) library.

To use a [MySQL database](https://docs.dagster.io/api/libraries/dagster-mysql) for storage, configure `storage.mysql` in `dagster.yaml`:

```codeBlockLines_e6Vv

# MySQL storage can be set using either credentials or a connection string.  This requires that the
# `dagster-mysql` library be installed.

# this config manually sets the MySQL credentials
storage:
  mysql:
    mysql_db:
      username: { DAGSTER_MYSQL_USERNAME }
      password: { DAGSTER_MYSQL_PASSWORD }
      hostname: { DAGSTER_MYSQL_HOSTNAME }
      db_name: { DAGSTER_MYSQL_DB }
      port: 3306

# and this config grabs the database credentials from environment variables
storage:
  mysql:
    mysql_db:
      username:
        env: DAGSTER_MYSQL_USERNAME
      password:
        env: DAGSTER_MYSQL_PASSWORD
      hostname:
        env: DAGSTER_MYSQL_HOSTNAME
      db_name:
        env: DAGSTER_MYSQL_DB
      port: 3306

# and this config sets the credentials via DB connection string / url:
storage:
  mysql:
    mysql_url: { MYSQL_DB_CONN_STRING }

# this config grabs the MySQL connection string from environment variables
storage:
  mysql:
    mysql_url:
      env: MYSQL_DB_CONN_STRING

```

### Run launcher [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#run-launcher "Direct link to Run launcher")

The `run_launcher` key allows you to configure the run launcher for your instance. Run launchers determine where runs are executed. You can use one of the Dagster-provided options or write your own custom run launcher. For more information, see " [Run launchers](https://docs.dagster.io/guides/deploy/execution/run-launchers)".

Refer to the following tabs for available options and sample configuration. Keep in mind that databases should be configured to use UTC timezone.

- DefaultRunLauncher (default)
- DockerRunLauncher
- K8sRunLauncher

**DefaultRunLauncher**

The [`DefaultRunLauncher`](https://docs.dagster.io/api/dagster/internals#dagster._core.launcher.DefaultRunLauncher) spawns a new process in the same node as a job's code location.

```codeBlockLines_e6Vv

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

```

**DockerRunLauncher**

The [`DockerRunLauncher`](https://docs.dagster.io/api/libraries/dagster-docker#dagster_docker.DockerRunLauncher) allocates a Docker container per run.

```codeBlockLines_e6Vv

run_launcher:
  module: dagster_docker
  class: DockerRunLauncher

```

**K8sRunLauncher**

The [`K8sRunLauncher`](https://docs.dagster.io/api/libraries/dagster-k8s#dagster_k8s.K8sRunLauncher) allocates a Kubernetes job per run.

```codeBlockLines_e6Vv

# there are multiple ways to configure the K8sRunLauncher

# you can set the follow configuration values directly
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name: pipeline_run_service_account
    job_image: my_project/dagster_image:latest
    instance_config_map: dagster-instance
    postgres_password_secret: dagster-postgresql-secret

# alternatively, you can grab any of these config values from environment variables:
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name:
      env: PIPELINE_RUN_SERVICE_ACCOUNT
    job_image:
      env: DAGSTER_IMAGE_NAME
    instance_config_map:
      env: DAGSTER_INSTANCE_CONFIG_MAP
    postgres_password_secret:
      env: DAGSTER_POSTGRES_SECRET

```

### Run coordinator [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#run-coordinator "Direct link to Run coordinator")

The `run_coordinator` key allows you to configure the run coordinator for your instance. Run coordinators determine the policy used to set the prioritization rules and concurrency limits for runs. For more information and troubleshooting help, see " [Run coordinators](https://docs.dagster.io/guides/deploy/execution/run-coordinators)".

Refer to the following tabs for available options and sample configuration.

- DefaultRunCoordinator (default)
- QueuedRunCoordinator

**DefaultRunCoordinator (default)**

The default run coordinator, the [`DefaultRunCoordinator`](https://docs.dagster.io/api/dagster/internals#dagster._core.run_coordinator.DefaultRunCoordinator) immediately sends runs to the [run launcher](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#run-launcher). There isn't a notion of `Queued` runs.

```codeBlockLines_e6Vv

# Since DefaultRunCoordinator is the default option, omitting the `run_coordinator` key will also suffice,
# but if you would like to set it explicitly:
run_coordinator:
  module: dagster.core.run_coordinator
  class: DefaultRunCoordinator

```

**QueuedRunCoordinator**

The [`QueuedRunCoordinator`](https://docs.dagster.io/api/dagster/internals#dagster._core.run_coordinator.QueuedRunCoordinator) allows you to set limits on the number of runs that can be executed at once. **Note** This requires an active [dagster-daemon process](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) to launch the runs.

This run coordinator supports both limiting the overall number of concurrent runs and specific limits based on run tags. For example, to avoid throttling, you can specify a concurrency limit for runs that interact with a specific cloud service.

```codeBlockLines_e6Vv

# There are a few ways to configure the QueuedRunCoordinator:

# this first option has concurrency limits set to default values
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator

# this second option manually specifies limits:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 25
    tag_concurrency_limits:
      - key: "database"
        value: "redshift"
        limit: 4
      - key: "dagster/backfill"
        limit: 10

# as always, some or all of these values can be obtained from environment variables:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs:
      env: DAGSTER_OVERALL_CONCURRENCY_LIMIT
    tag_concurrency_limits:
      - key: "database"
        value: "redshift"
        limit:
          env: DAGSTER_REDSHIFT_CONCURRENCY_LIMIT
      - key: "dagster/backfill"
        limit:
          env: DAGSTER_BACKFILL_CONCURRENCY_LIMIT

# for higher dequeue throughput, threading can be enabled:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    dequeue_use_threads: true
    dequeue_num_workers: 8

```

### Compute log storage [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#compute-log-storage "Direct link to Compute log storage")

The `compute_logs` key allows you to configure compute log storage. Compute log storage controls the capture and persistence of raw `stdout` and `stderr` text logs.

Refer to the following tabs for available options and sample configuration.

- LocalComputeLogManager (default)
- NoOpComputeLogManager
- AzureBlobComputeLogManager
- GCSComputeLogManager
- S3ComputeLogManager

**LocalComputeLogManager**

Used by default, the [`LocalComputeLogManager`](https://docs.dagster.io/api/dagster/internals#dagster._core.storage.local_compute_log_manager.LocalComputeLogManager) writes `stdout` and `stderr` logs to disk.

```codeBlockLines_e6Vv

# there are two ways to set the directory that the LocalComputeLogManager writes
# stdout & stderr logs to

# You could directly set the `base_dir` key
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /path/to/directory

# Alternatively, you could set the `base_dir` key to an environment variable
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir:
      env: LOCAL_COMPUTE_LOG_MANAGER_DIRECTORY

```

**NoOpComputeLogManager**

The [`NoOpComputeLogManager`](https://docs.dagster.io/api/dagster/internals#dagster._core.storage.noop_compute_log_manager.NoOpComputeLogManager) does not store `stdout` and `stderr` logs for any step.

```codeBlockLines_e6Vv

compute_logs:
  module: dagster.core.storage.noop_compute_log_manager
  class: NoOpComputeLogManager

```

**AzureBlobComputeLogManager**

The [`blob.AzureBlobComputeLogManager`](https://docs.dagster.io/api/libraries/dagster-azure#dagster_azure.blob.AzureBlobComputeLogManager) writes `stdout` and `stderr` to Azure Blob Storage.

```codeBlockLines_e6Vv
# there are multiple ways to configure the AzureBlobComputeLogManager

# you can set the necessary configuration values directly:
compute_logs:
  module: dagster_azure.blob.compute_log_manager
  class: AzureBlobComputeLogManager
  config:
    storage_account: mycorp-dagster
    container: compute-logs
    secret_credential:
      client_id: ...
      tenant_id: ...
      client_secret: ...
    local_dir: /tmp/bar
    prefix: dagster-test-

# alternatively, you can obtain any of these config values from environment variables
compute_logs:
  module: dagster_azure.blob.compute_log_manager
  class: AzureBlobComputeLogManager
  config:
    storage_account:
      env: MYCORP_DAGSTER_STORAGE_ACCOUNT_NAME
    container:
      env: CONTAINER_NAME
    secret_credential:
      client_id: ...
      tenant_id: ...
      client_secret: ...
    local_dir:
      env: LOCAL_DIR_PATH
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

```

**GCSComputeLogManager**

The [`gcs.GCSComputeLogManager`](https://docs.dagster.io/api/libraries/dagster-gcp#dagster_gcp.gcs.GCSComputeLogManager) writes `stdout` and `stderr` to Google Cloud Storage.

```codeBlockLines_e6Vv
# there are multiple ways to configure the GCSComputeLogManager

# you can set the necessary configuration values directly:
compute_logs:
  module: dagster_gcp.gcs.compute_log_manager
  class: GCSComputeLogManager
  config:
    bucket: mycorp-dagster-compute-logs
    prefix: dagster-test-

# alternatively, you can obtain any of these config values from environment variables
compute_logs:
  module: dagster_gcp.gcs.compute_log_manager
  class: GCSComputeLogManager
  config:
    bucket:
      env: MYCORP_DAGSTER_COMPUTE_LOGS_BUCKET
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

```

**S3ComputeLogManager**

The [`s3.S3ComputeLogManager`](https://docs.dagster.io/api/libraries/dagster-aws#dagster_aws.s3.S3ComputeLogManager) writes `stdout` and `stderr` to an Amazon Web Services S3 bucket.

```codeBlockLines_e6Vv

# there are multiple ways to configure the S3ComputeLogManager

# you can set the config values directly:
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket: "mycorp-dagster-compute-logs"
    prefix: "dagster-test-"

# or grab some or all of them from environment variables
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket:
      env: MYCORP_DAGSTER_COMPUTE_LOGS_BUCKET
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

```

### Local artifact storage [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#local-artifact-storage "Direct link to Local artifact storage")

The `local_artifact_storage` key allows you to configure local artifact storage. Local artifact storage is used to:

- Configure storage for artifacts that require a local disk, or
- Store inputs and outputs when using the filesystem I/O manager ( [`FilesystemIOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.FilesystemIOManager)). For more information on how other I/O managers store artifacts, see the [I/O managers documentation](https://docs.dagster.io/guides/build/io-managers/).

note

[`LocalArtifactStorage`](https://docs.dagster.io/api/dagster/internals#dagster._core.storage.root.LocalArtifactStorage)

is currently the only option for local artifact storage. This option configures the directory used by the default
filesystem I/O Manager, as well as any artifacts that require a local disk.

```codeBlockLines_e6Vv
# there are two possible ways to configure LocalArtifactStorage

# example local_artifact_storage setup pointing to /var/shared/dagster directory
local_artifact_storage:
  module: dagster.core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: "/path/to/dir"

# alternatively, `base_dir` can be set to an environment variable
local_artifact_storage:
  module: dagster.core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir:
      env: DAGSTER_LOCAL_ARTIFACT_STORAGE_DIR

```

### Telemetry [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#telemetry "Direct link to Telemetry")

The `telemetry` key allows you to opt in or out of Dagster collecting anonymized usage statistics. This is set to `true` by default.

```codeBlockLines_e6Vv

# Allows opting out of Dagster collecting usage statistics.
telemetry:
  enabled: false

```

For more information, see the [Telemetry documentation](https://docs.dagster.io/about/telemetry).

### gRPC servers [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#grpc-servers "Direct link to gRPC servers")

The `code_servers` key allows you to configure how Dagster loads the code in a [code location](https://docs.dagster.io/guides/deploy/code-locations/).

When you aren't [running your own gRPC server](https://docs.dagster.io/guides/deploy/code-locations/workspace-yaml#grpc-server), the webserver and the Dagster daemon load your code from a gRPC server running in a subprocess. By default, if your code takes more than 180 seconds to load, Dagster assumes that it's hanging and stops waiting for it to load.

If you expect that your code will take longer than 180 seconds to load, set the `code_servers.local_startup_timeout` key. The value should be an integer that indicates the maximum timeout, in seconds.

```codeBlockLines_e6Vv

# Configures how long Dagster waits for code locations
# to load before timing out.
code_servers:
  local_startup_timeout: 360

```

### Data retention [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#data-retention "Direct link to Data retention")

The `retention` key allows you to configure how long Dagster retains certain types of data. Specifically, data that has diminishing value over time, such as schedule/sensor tick data. Cleaning up old ticks can help minimize storage concerns and improve query performance.

By default, Dagster retains skipped sensor ticks for seven days and all other tick types indefinitely. To customize the retention policies for schedule and sensor ticks, use the `purge_after_days` key:

```codeBlockLines_e6Vv

# Configures how long Dagster keeps sensor / schedule tick data
retention:
  schedule:
    purge_after_days: 90 # sets retention policy for schedule ticks of all types
  sensor:
    purge_after_days:
      skipped: 7
      failure: 30
      success: -1 # keep success ticks indefinitely

```

The `purge_after_days` key accepts either:

- A single integer that indicates how long, in days, to retain ticks of all types. **Note**: A value of `-1` retains ticks indefinitely.
- A mapping of tick types ( `skipped`, `failure`, `success`) to integers. The integers indicate how long, in days, to retain the tick type.

### Sensor evaluation [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#sensor-evaluation "Direct link to Sensor evaluation")

The `sensors` key allows you to configure how sensors are evaluated. To evaluate multiple sensors in parallel simultaneously, set the `use_threads` and `num_workers` keys:

```codeBlockLines_e6Vv

sensors:
  use_threads: true
  num_workers: 8

```

You can also set the optional `num_submit_workers` key to evaluate multiple run requests from the same sensor tick in parallel, which can help decrease latency when a single sensor tick returns many run requests.

### Schedule evaluation [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#schedule-evaluation "Direct link to Schedule evaluation")

The `schedules` key allows you to configure how schedules are evaluated. By default, Dagster evaluates schedules one at a time.

To evaluate multiple schedules in parallel simultaneously, set the `use_threads` and `num_workers` keys:

```codeBlockLines_e6Vv

schedules:
  use_threads: true
  num_workers: 8

```

You can also set the optional `num_submit_workers` key to evaluate multiple run requests from the same schedule tick in parallel, which can help decrease latency when a single schedule tick returns many run requests.

### Backfill processing [​](https://docs.dagster.io/guides/deploy/dagster-instance-configuration\#backfill-processing "Direct link to Backfill processing")

The `backfills` key allows you to configure how backfills are processed. By default, Dagster processes backfills one at a time.

To process multiple backfills in parallel simultaneously, set the `use_threads` and `num_workers` keys:

```codeBlockLines_e6Vv
# start_marker_storage_sqlite

# there are two ways to set storage to SqliteStorage

# this config manually sets the directory (`base_dir`) for Sqlite to store data in:
storage:
  sqlite:
    base_dir: /path/to/dir

# and this config grabs the directory from an environment variable
storage:
  sqlite:
    base_dir:
      env: SQLITE_STORAGE_BASE_DIR

# end_marker_storage_sqlite

# start_marker_storage_postgres

# Postgres storage can be set using either credentials or a connection string.  This requires that
# the `dagster-postgres` library be installed and a database configured with UTC timezone.

# this config manually sets the Postgres credentials
storage:
  postgres:
    postgres_db:
      username: { DAGSTER_PG_USERNAME }
      password: { DAGSTER_PG_PASSWORD }
      hostname: { DAGSTER_PG_HOSTNAME }
      db_name: { DAGSTER_PG_DB }
      port: 5432

# and this config grabs the database credentials from environment variables
storage:
  postgres:
    postgres_db:
      username:
        env: DAGSTER_PG_USERNAME
      password:
        env: DAGSTER_PG_PASSWORD
      hostname:
        env: DAGSTER_PG_HOST
      db_name:
        env: DAGSTER_PG_DB
      port: 5432

# and this config sets the credentials via DB connection string / url:
storage:
  postgres:
    postgres_url: { PG_DB_CONN_STRING }

# This config gets the DB connection string / url via environment variables:
storage:
  postgres:
    postgres_url:
      env: PG_DB_CONN_STRING

# end_marker_storage_postgres

# start_marker_storage_mysql

# MySQL storage can be set using either credentials or a connection string.  This requires that the
# `dagster-mysql` library be installed.

# this config manually sets the MySQL credentials
storage:
  mysql:
    mysql_db:
      username: { DAGSTER_MYSQL_USERNAME }
      password: { DAGSTER_MYSQL_PASSWORD }
      hostname: { DAGSTER_MYSQL_HOSTNAME }
      db_name: { DAGSTER_MYSQL_DB }
      port: 3306

# and this config grabs the database credentials from environment variables
storage:
  mysql:
    mysql_db:
      username:
        env: DAGSTER_MYSQL_USERNAME
      password:
        env: DAGSTER_MYSQL_PASSWORD
      hostname:
        env: DAGSTER_MYSQL_HOSTNAME
      db_name:
        env: DAGSTER_MYSQL_DB
      port: 3306

# and this config sets the credentials via DB connection string / url:
storage:
  mysql:
    mysql_url: { MYSQL_DB_CONN_STRING }

# this config grabs the MySQL connection string from environment variables
storage:
  mysql:
    mysql_url:
      env: MYSQL_DB_CONN_STRING

# end_marker_storage_mysql

# start_marker_run_launcher_default

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

# end_marker_run_launcher_default

# start_marker_run_launcher_docker

run_launcher:
  module: dagster_docker
  class: DockerRunLauncher

# end_marker_run_launcher_docker

# start_marker_run_launcher_k8s

# there are multiple ways to configure the K8sRunLauncher

# you can set the follow configuration values directly
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name: pipeline_run_service_account
    job_image: my_project/dagster_image:latest
    instance_config_map: dagster-instance
    postgres_password_secret: dagster-postgresql-secret

# alternatively, you can grab any of these config values from environment variables:
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name:
      env: PIPELINE_RUN_SERVICE_ACCOUNT
    job_image:
      env: DAGSTER_IMAGE_NAME
    instance_config_map:
      env: DAGSTER_INSTANCE_CONFIG_MAP
    postgres_password_secret:
      env: DAGSTER_POSTGRES_SECRET

# end_marker_run_launcher_k8s

# start_marker_run_coordinator_default

# Since DefaultRunCoordinator is the default option, omitting the `run_coordinator` key will also suffice,
# but if you would like to set it explicitly:
run_coordinator:
  module: dagster.core.run_coordinator
  class: DefaultRunCoordinator

# end_marker_run_coordinator_default

# start_marker_run_coordinator_queued

# There are a few ways to configure the QueuedRunCoordinator:

# this first option has concurrency limits set to default values
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator

# this second option manually specifies limits:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 25
    tag_concurrency_limits:
      - key: "database"
        value: "redshift"
        limit: 4
      - key: "dagster/backfill"
        limit: 10

# as always, some or all of these values can be obtained from environment variables:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs:
      env: DAGSTER_OVERALL_CONCURRENCY_LIMIT
    tag_concurrency_limits:
      - key: "database"
        value: "redshift"
        limit:
          env: DAGSTER_REDSHIFT_CONCURRENCY_LIMIT
      - key: "dagster/backfill"
        limit:
          env: DAGSTER_BACKFILL_CONCURRENCY_LIMIT

# for higher dequeue throughput, threading can be enabled:
run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    dequeue_use_threads: true
    dequeue_num_workers: 8

# end_marker_run_coordinator_queued

# start_marker_compute_log_storage_local

# there are two ways to set the directory that the LocalComputeLogManager writes
# stdout & stderr logs to

# You could directly set the `base_dir` key
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /path/to/directory

# Alternatively, you could set the `base_dir` key to an environment variable
compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir:
      env: LOCAL_COMPUTE_LOG_MANAGER_DIRECTORY

# end_marker_compute_log_storage_local

# start_marker_compute_log_storage_noop

compute_logs:
  module: dagster.core.storage.noop_compute_log_manager
  class: NoOpComputeLogManager

# end_marker_compute_log_storage_noop

# start_marker_compute_log_storage_blob
# there are multiple ways to configure the AzureBlobComputeLogManager

# you can set the necessary configuration values directly:
compute_logs:
  module: dagster_azure.blob.compute_log_manager
  class: AzureBlobComputeLogManager
  config:
    storage_account: mycorp-dagster
    container: compute-logs
    secret_credential:
      client_id: ...
      tenant_id: ...
      client_secret: ...
    local_dir: /tmp/bar
    prefix: dagster-test-

# alternatively, you can obtain any of these config values from environment variables
compute_logs:
  module: dagster_azure.blob.compute_log_manager
  class: AzureBlobComputeLogManager
  config:
    storage_account:
      env: MYCORP_DAGSTER_STORAGE_ACCOUNT_NAME
    container:
      env: CONTAINER_NAME
    secret_credential:
      client_id: ...
      tenant_id: ...
      client_secret: ...
    local_dir:
      env: LOCAL_DIR_PATH
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

# end_marker_compute_log_storage_blob

# start_marker_compute_log_storage_s3

# there are multiple ways to configure the S3ComputeLogManager

# you can set the config values directly:
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket: "mycorp-dagster-compute-logs"
    prefix: "dagster-test-"

# or grab some or all of them from environment variables
compute_logs:
  module: dagster_aws.s3.compute_log_manager
  class: S3ComputeLogManager
  config:
    bucket:
      env: MYCORP_DAGSTER_COMPUTE_LOGS_BUCKET
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

# end_marker_compute_log_storage_s3

# start_marker_compute_log_storage_gcs
# there are multiple ways to configure the GCSComputeLogManager

# you can set the necessary configuration values directly:
compute_logs:
  module: dagster_gcp.gcs.compute_log_manager
  class: GCSComputeLogManager
  config:
    bucket: mycorp-dagster-compute-logs
    prefix: dagster-test-

# alternatively, you can obtain any of these config values from environment variables
compute_logs:
  module: dagster_gcp.gcs.compute_log_manager
  class: GCSComputeLogManager
  config:
    bucket:
      env: MYCORP_DAGSTER_COMPUTE_LOGS_BUCKET
    prefix:
      env: DAGSTER_COMPUTE_LOG_PREFIX

# end_marker_compute_log_storage_gcs

# start_marker_local_artifact_storage
# there are two possible ways to configure LocalArtifactStorage

# example local_artifact_storage setup pointing to /var/shared/dagster directory
local_artifact_storage:
  module: dagster.core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: "/path/to/dir"

# alternatively, `base_dir` can be set to an environment variable
local_artifact_storage:
  module: dagster.core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir:
      env: DAGSTER_LOCAL_ARTIFACT_STORAGE_DIR

# end_marker_local_artifact_storage

# start_run_monitoring

# Opt in to run monitoring
run_monitoring:
  enabled: true
  # values below are the defaults, and don't need to be specified except to override them
  start_timeout_seconds: 180
  cancel_timeout_seconds: 180
  max_resume_run_attempts: 3
  poll_interval_seconds: 120

# end_run_monitoring

# start_run_retries
run_retries:
  enabled: true
  max_retries: 3 # Sets a default for all jobs. 0 if not set
# end_run_retries

# start_marker_telemetry

# Allows opting out of Dagster collecting usage statistics.
telemetry:
  enabled: false

# end_marker_telemetry

# start_marker_code_servers

# Configures how long Dagster waits for code locations
# to load before timing out.
code_servers:
  local_startup_timeout: 360

# end_marker_code_servers

# start_marker_retention

# Configures how long Dagster keeps sensor / schedule tick data
retention:
  schedule:
    purge_after_days: 90 # sets retention policy for schedule ticks of all types
  sensor:
    purge_after_days:
      skipped: 7
      failure: 30
      success: -1 # keep success ticks indefinitely

# end_marker_retention

# start_marker_sensors

sensors:
  use_threads: true
  num_workers: 8

# end_marker_sensors

# start_marker_schedules

schedules:
  use_threads: true
  num_workers: 8

# end_marker_schedules

auto_materialize:
  run_tags:

```

You can also set the optional `num_submit_workers` key to create multiple runs from the same backfill in parallel, which can help decrease latency when a single backfill creates many runs.

- [Default local behavior](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#default-local-behavior)
- [Configuration reference](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#configuration-reference)
  - [Dagster storage](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#dagster-storage)
  - [Run launcher](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#run-launcher)
  - [Run coordinator](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#run-coordinator)
  - [Compute log storage](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#compute-log-storage)
  - [Local artifact storage](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#local-artifact-storage)
  - [Telemetry](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#telemetry)
  - [gRPC servers](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#grpc-servers)
  - [Data retention](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#data-retention)
  - [Sensor evaluation](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#sensor-evaluation)
  - [Schedule evaluation](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#schedule-evaluation)
  - [Backfill processing](https://docs.dagster.io/guides/deploy/dagster-instance-configuration#backfill-processing)
