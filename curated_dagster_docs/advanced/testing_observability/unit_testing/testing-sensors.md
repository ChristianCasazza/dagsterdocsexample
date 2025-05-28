
- Via the Dagster UI
- Via the CLI
- Via Python

**Via the Dagster UI**

note

**Before you test:** Test evaluations of sensors run the sensor's underlying Python function, meaning that any side effects contained within that sensor's function may be executed.

In the UI, you can manually trigger a test evaluation of a sensor and view the results.

1. Click **Overview > Sensors**.

2. Click the sensor you want to test.

3. Click the **Preview tick result** button, located near the top right corner of the page.

![Test sensor button](https://docs.dagster.io/assets/images/test-sensor-button-4776007b3220e10a8ec164e14f845a23.png)

4. You'll be prompted to provide a cursor value (optional). You can use the existing cursor for the sensor (which will be prepopulated) or enter a different value. If you're not using cursors, leave this field blank.

![Cursor value field](https://docs.dagster.io/assets/images/provide-cursor-page-dff638689c90157757ee2873c9f0d288.png)

5. Click **Continue** to fire the sensor. A window containing the result of the evaluation will display, whether it's run requests, a skip reason, or a Python error:

![Evaluation result page](https://docs.dagster.io/assets/images/eval-result-page-5775c72b153aca58b7afc9f944b35ffb.png)

6. If the preview was successful, then for each produced run request, you can view the run config and tags produced by that run request by clicking the  button in the Actions column.

![Actions page in the Dagster UI](https://docs.dagster.io/assets/images/actions-page-a0c5fa0d0ce13d8dccece7a7dd996721.png)

7. Click the **Launch all & commit tick result** on the bottom right to launch all the run requests. It will launch the runs and link to the /runs page filtered to the IDs of the runs that launched.

![Runs page after launching all runs in the Dagster UI](https://docs.dagster.io/assets/images/launch-all-page-d940675602f23e8925ae6e481a29b3b4.png)


**Via the CLI**

To quickly preview what an existing sensor will generate when evaluated, run the following::

```codeBlockLines_e6Vv
dagster sensor preview my_sensor_name

```

**Via Python**

To unit test sensors, you can directly invoke the sensor's Python function. This will return all the run requests yielded by the sensor. The config obtained from the returned run requests can be validated using the [`validate_run_config`](https://docs.dagster.io/api/dagster/execution#dagster.validate_run_config) function:

```codeBlockLines_e6Vv
from dagster import validate_run_config

@sensor(target=log_file_job)
def sensor_to_test():
    yield RunRequest(
        run_key="foo",
        run_config={"ops": {"process_file": {"config": {"filename": "foo"}}}},
    )

def test_sensor():
    for run_request in sensor_to_test():
        assert validate_run_config(log_file_job, run_request.run_config)

```

Notice that since the context argument wasn't used in the sensor, a context object doesn't have to be provided. However, if the context object **is** needed, it can be provided via [`build_sensor_context`](https://docs.dagster.io/api/dagster/schedules-sensors#dagster.build_sensor_context). Consider again the `my_directory_sensor_cursor` example:

```codeBlockLines_e6Vv
@sensor(target=log_file_job)
def my_directory_sensor_cursor(context):
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime <= last_mtime:
                continue

            # the run key should include mtime if we want to kick off new runs based on file modifications
            run_key = f"{filename}:{file_mtime}"
            run_config = {"ops": {"process_file": {"config": {"filename": filename}}}}
            yield RunRequest(run_key=run_key, run_config=run_config)
            max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))

```

This sensor uses the `context` argument. To invoke it, we need to provide one:

```codeBlockLines_e6Vv
from dagster import build_sensor_context

def test_my_directory_sensor_cursor():
    context = build_sensor_context(cursor="0")
    for run_request in my_directory_sensor_cursor(context):
        assert validate_run_config(log_file_job, run_request.run_config)

```

**Testing sensors with resources**

For sensors which utilize [resources](https://docs.dagster.io/guides/build/external-resources/), you can provide the necessary resources when invoking the sensor function.

Below is a test for the `process_new_users_sensor` that we defined in " [Using resources in sensors](https://docs.dagster.io/guides/automate/sensors/using-resources-in-sensors)", which uses the `users_api` resource.

```codeBlockLines_e6Vv

from dagster import build_sensor_context

def test_process_new_users_sensor():
    class FakeUsersAPI:
        def fetch_users(self) -> list[str]:
            return ["1", "2", "3"]

    context = build_sensor_context()
    run_requests = process_new_users_sensor(context, users_api=FakeUsersAPI())
    assert len(run_requests) == 3

```
