
On this page

## Quickstart [​](https://docs.dagster.io/api/libraries/dagster-celery\#quickstart "Direct link to Quickstart")

To get a local rabbitmq broker started and available via the default
`pyamqp://guest@localhost:5672`, in the `dagster/python_modules/libraries/dagster-celery/`
directory run:

```codeBlockLines_e6Vv
docker-compose up

```

To run a celery worker:

```codeBlockLines_e6Vv
celery -A dagster_celery.app worker -l info

```

To start multiple workers in the background, run:

```codeBlockLines_e6Vv
celery multi start w2 -A dagster_celery.app -l info

```

To execute a job using the celery-backed executor, you’ll need to set the job’s `executor_def` to
the celery\_executor.

```codeBlockLines_e6Vv
from dagster import job
from dagster_celery import celery_executor

@job(executor_def=celery_executor)
def my_job():
    pass

```

### Monitoring your Celery tasks [​](https://docs.dagster.io/api/libraries/dagster-celery\#monitoring-your-celery-tasks "Direct link to Monitoring your Celery tasks")

We advise using [Flower](https://celery.readthedocs.io/en/latest/userguide/monitoring.html#flower-real-time-celery-web-monitor):

```codeBlockLines_e6Vv
celery -A dagster_celery.app flower

```

### Customizing the Celery broker, backend, and other app configuration [​](https://docs.dagster.io/api/libraries/dagster-celery\#customizing-the-celery-broker-backend-and-other-app-configuration "Direct link to Customizing the Celery broker, backend, and other app configuration")

By default this will use `amqp://guest:**@localhost:5672//` as the Celery broker URL and
`rpc://` as the results backend. In production, you will want to change these values. Pending the
introduction of a dagster\_celery CLI, that would entail writing a Python module `my_module` as
follows:

```codeBlockLines_e6Vv
from celery import Celery

from dagster_celery.tasks import create_task

app = Celery('dagster', broker_url='some://custom@value', ...)

execute_plan = create_task(app)

if __name__ == '__main__':
    app.worker_main()

```

You can then run the celery worker using:

```codeBlockLines_e6Vv
celery -A my_module worker --loglevel=info

```

This customization mechanism is used to implement dagster\_celery\_k8s and dagster\_celery\_k8s which delegate the execution of steps to ephemeral kubernetes pods and docker containers, respectively.

## API [​](https://docs.dagster.io/api/libraries/dagster-celery\#api "Direct link to API")

dagster\_celery.celery\_executor ExecutorDefinition  [\[source\]](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-celery/dagster_celery/executor.py#L46)

Celery-based executor.

The Celery executor exposes config settings for the underlying Celery app under
the `config_source` key. This config corresponds to the “new lowercase settings” introduced
in Celery version 4.0 and the object constructed from config will be passed to the
`celery.Celery` constructor as its `config_source` argument.
(See [https://docs.celeryq.dev/en/stable/userguide/configuration.html](https://docs.celeryq.dev/en/stable/userguide/configuration.html) for details.)

The executor also exposes the `broker`, backend, and `include` arguments to the
`celery.Celery` constructor.

In the most common case, you may want to modify the `broker` and `backend` (e.g., to use
Redis instead of RabbitMQ). We expect that `config_source` will be less frequently
modified, but that when solid executions are especially fast or slow, or when there are
different requirements around idempotence or retry, it may make sense to execute jobs
with variations on these settings.

To use the celery\_executor, set it as the executor\_def when defining a job:

```codeBlockLines_e6Vv
from dagster import job
from dagster_celery import celery_executor

@job(executor_def=celery_executor)
def celery_enabled_job():
    pass

```

Then you can configure the executor as follows:

```codeBlockLines_e6Vv
execution:
  config:
    broker: 'pyamqp://guest@localhost//'  # Optional[str]: The URL of the Celery broker
    backend: 'rpc://' # Optional[str]: The URL of the Celery results backend
    include: ['my_module'] # Optional[List[str]]: Modules every worker should import
    config_source: # Dict[str, Any]: Any additional parameters to pass to the
        #...       # Celery workers. This dict will be passed as the `config_source`
        #...       # argument of celery.Celery().

```

Note that the YAML you provide here must align with the configuration with which the Celery
workers on which you hope to run were started. If, for example, you point the executor at a
different broker than the one your workers are listening to, the workers will never be able to
pick up tasks for execution.

## CLI [​](https://docs.dagster.io/api/libraries/dagster-celery\#cli "Direct link to CLI")

The `dagster-celery` CLI lets you start, monitor, and terminate workers.

### dagster-celery worker start [​](https://docs.dagster.io/api/libraries/dagster-celery\#dagster-celery-worker-start "Direct link to dagster-celery worker start")

Start a dagster celery worker.

```codeBlockLines_e6Vv
dagster-celery worker start [OPTIONS] [ADDITIONAL_ARGS]...

```

Options:

-n, --name <name>

The name of the worker. Defaults to a unique name prefixed with “dagster-” and ending with the hostname.

-y, --config-yaml <config\_yaml>

Specify the path to a config YAML file with options for the worker. This is the same config block that you provide to dagster\_celery.celery\_executor when configuring a job for execution with Celery, with, e.g., the URL of the broker to use.

-q, --queue <queue>

Names of the queues on which this worker should listen for tasks. Provide multiple -q arguments to specify multiple queues. Note that each celery worker may listen on no more than four queues.

-d, --background

Set this flag to run the worker in the background.

-i, --includes <includes>

Python modules the worker should import. Provide multiple -i arguments to specify multiple modules.

-l, --loglevel <loglevel>

Log level for the worker.

-A, --app <app>

Arguments:

ADDITIONAL\_ARGS

Optional argument(s)

### dagster-celery worker list [​](https://docs.dagster.io/api/libraries/dagster-celery\#dagster-celery-worker-list "Direct link to dagster-celery worker list")

List running dagster-celery workers. Note that we use the broker to contact the workers.

```codeBlockLines_e6Vv
dagster-celery worker list [OPTIONS]

```

Options:

-y, --config-yaml <config\_yaml>

Specify the path to a config YAML file with options for the workers you are trying to manage. This is the same config block that you provide to dagster\_celery.celery\_executor when configuring a job for execution with Celery, with, e.g., the URL of the broker to use. Without this config file, you will not be able to find your workers (since the CLI won’t know how to reach the broker).

### dagster-celery worker terminate [​](https://docs.dagster.io/api/libraries/dagster-celery\#dagster-celery-worker-terminate "Direct link to dagster-celery worker terminate")

Shut down dagster-celery workers. Note that we use the broker to send signals to the workers to terminate – if the broker is not running, this command is a no-op. Provide the argument NAME to terminate a specific worker by name.

```codeBlockLines_e6Vv
dagster-celery worker terminate [OPTIONS] [NAME]

```

Options:

-a, --all

Set this flag to terminate all running workers.

-y, --config-yaml <config\_yaml>

Specify the path to a config YAML file with options for the workers you are trying to manage. This is the same config block that you provide to dagster\_celery.celery\_executor when configuring a job for execution with Celery, with, e.g., the URL of the broker to use. Without this config file, you will not be able to terminate your workers (since the CLI won’t know how to reach the broker).

Arguments:

NAME

Optional argument

- [Quickstart](https://docs.dagster.io/api/libraries/dagster-celery#quickstart)
  - [Monitoring your Celery tasks](https://docs.dagster.io/api/libraries/dagster-celery#monitoring-your-celery-tasks)
  - [Customizing the Celery broker, backend, and other app configuration](https://docs.dagster.io/api/libraries/dagster-celery#customizing-the-celery-broker-backend-and-other-app-configuration)
- [API](https://docs.dagster.io/api/libraries/dagster-celery#api)
- [CLI](https://docs.dagster.io/api/libraries/dagster-celery#cli)
  - [dagster-celery worker start](https://docs.dagster.io/api/libraries/dagster-celery#dagster-celery-worker-start)
  - [dagster-celery worker list](https://docs.dagster.io/api/libraries/dagster-celery#dagster-celery-worker-list)
  - [dagster-celery worker terminate](https://docs.dagster.io/api/libraries/dagster-celery#dagster-celery-worker-terminate)
