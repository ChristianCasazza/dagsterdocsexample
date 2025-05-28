
On this page

See also the [Kubernetes deployment guide](https://docs.dagster.io/guides/deploy/deployment-options/kubernetes).

This library contains utilities for running Dagster with Kubernetes. This includes a Python API
allowing the webserver to launch runs as Kubernetes Jobs, as well as a Helm chart you can use as the basis
for a Dagster deployment on a Kubernetes cluster.

## APIs [​](https://docs.dagster.io/api/libraries/dagster-k8s\#apis "Direct link to APIs")

dagster\_k8s.K8sRunLauncher RunLauncher

RunLauncher that starts a Kubernetes Job for each Dagster job run.

Encapsulates each run in a separate, isolated invocation of `dagster-graphql`.

You can configure a Dagster instance to use this RunLauncher by adding a section to your
`dagster.yaml` like the following:

```codeBlockLines_e6Vv
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    service_account_name: your_service_account
    job_image: my_project/dagster_image:latest
    instance_config_map: dagster-instance
    postgres_password_secret: dagster-postgresql-secret

```

dagster\_k8s.k8s\_job\_executor ExecutorDefinition

Executor which launches steps as Kubernetes Jobs.

To use the k8s\_job\_executor, set it as the executor\_def when defining a job:

```codeBlockLines_e6Vv
from dagster_k8s import k8s_job_executor

from dagster import job

@job(executor_def=k8s_job_executor)
def k8s_job():
    pass

```

Then you can configure the executor with run config as follows:

```codeBlockLines_e6Vv
execution:
  config:
    job_namespace: 'some-namespace'
    image_pull_policy: ...
    image_pull_secrets: ...
    service_account_name: ...
    env_config_maps: ...
    env_secrets: ...
    env_vars: ...
    job_image: ... # leave out if using userDeployments
    max_concurrent: ...

```

max\_concurrent limits the number of pods that will execute concurrently for one run. By default
there is no limit- it will maximally parallel as allowed by the DAG. Note that this is not a
global limit.

Configuration set on the Kubernetes Jobs and Pods created by the K8sRunLauncher will also be
set on Kubernetes Jobs and Pods created by the k8s\_job\_executor.

Configuration set using tags on a @job will only apply to the run level. For configuration
to apply at each step it must be set using tags for each @op.

## Ops [​](https://docs.dagster.io/api/libraries/dagster-k8s\#ops "Direct link to Ops")

dagster\_k8s.k8s\_job\_op `=` <dagster.\_core.definitions.op\_definition.OpDefinition object>

beta

This API is currently in beta, and may have breaking changes in minor version releases, with behavior changes in patch releases.

An op that runs a Kubernetes job using the k8s API.

Contrast with the k8s\_job\_executor, which runs each Dagster op in a Dagster job in its
own k8s job.

This op may be useful when:

- You need to orchestrate a command that isn’t a Dagster op (or isn’t written in Python)
- You want to run the rest of a Dagster job using a specific executor, and only a single op in k8s.

For example:

```codeBlockLines_e6Vv
from dagster_k8s import k8s_job_op

from dagster import job

first_op = k8s_job_op.configured(
    {
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo HELLO"],
    },
    name="first_op",
)
second_op = k8s_job_op.configured(
    {
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo GOODBYE"],
    },
    name="second_op",
)

@job
def full_job():
    second_op(first_op())

```

You can create your own op with the same implementation by calling the execute\_k8s\_job function
inside your own op.

The service account that is used to run this job should have the following RBAC permissions:

```codeBlockLines_e6Vv
rules:
  - apiGroups: ["batch"]
      resources: ["jobs", "jobs/status"]
      verbs: ["*"]
  # The empty arg "" corresponds to the core API group
  - apiGroups: [""]
      resources: ["pods", "pods/log", "pods/status"]
      verbs: ["*"]'

```

dagster\_k8s.execute\_k8s\_job

beta

This API is currently in beta, and may have breaking changes in minor version releases, with behavior changes in patch releases.

This function is a utility for executing a Kubernetes job from within a Dagster op.

Parameters:

- **image** ( _str_) – The image in which to launch the k8s job.
- **command** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – The command to run in the container within the launched k8s job. Default: None.
- **args** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – The args for the command for the container. Default: None.
- **namespace** ( _Optional_ _\[_ _str_ _\]_) – Override the kubernetes namespace in which to run the k8s job. Default: None.
- **image\_pull\_policy** ( _Optional_ _\[_ _str_ _\]_) – Allows the image pull policy to be overridden, e.g. to facilitate local testing with [kind](https://kind.sigs.k8s.io/). Default: `"Always"`. See: [https://kubernetes.io/docs/concepts/containers/images/#updating-images](https://kubernetes.io/docs/concepts/containers/images/#updating-images).
- **image\_pull\_secrets** ( _Optional_ _\[_ _List_ _\[_ _Dict_ _\[_ _str_ _,_ _str_ _\]_ _\]_ _\]_) – Optionally, a list of dicts, each of which corresponds to a Kubernetes `LocalObjectReference` (e.g., `\{'name': 'myRegistryName'}`). This allows you to specify the \`\`imagePullSecrets\` on a pod basis. Typically, these will be provided through the service account, when needed, and you will not need to pass this argument. See: [https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod](https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod) and [https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.17/#podspec-v1-core](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.17/#podspec-v1-core)
- **service\_account\_name** ( _Optional_ _\[_ _str_ _\]_) – The name of the Kubernetes service account under which to run the Job. Defaults to “default” env\_config\_maps (Optional\[List\[str\]\]): A list of custom ConfigMapEnvSource names from which to draw environment variables (using `envFrom`) for the Job. Default: `[]`. See: [https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/#define-an-environment-variable-for-a-container](https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/#define-an-environment-variable-for-a-container)
- **env\_secrets** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – A list of custom Secret names from which to draw environment variables (using `envFrom`) for the Job. Default: `[]`. See: [https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables](https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables)
- **env\_vars** ( _Optional_ _\[_ _List_ _\[_ _str_ _\]_ _\]_) – A list of environment variables to inject into the Job. Default: `[]`. See: [https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables](https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/#configure-all-key-value-pairs-in-a-secret-as-container-environment-variables)
- **volume\_mounts** ( _Optional_ _\[_ _List_ _\[_ [_Permissive_](https://docs.dagster.io/api/dagster/config#dagster.Permissive) _\]_ _\]_) – A list of volume mounts to include in the job’s container. Default: `[]`. See: [https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volumemount-v1-core](https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volumemount-v1-core)
- **volumes** ( _Optional_ _\[_ _List_ _\[_ [_Permissive_](https://docs.dagster.io/api/dagster/config#dagster.Permissive) _\]_ _\]_) – A list of volumes to include in the Job’s Pod. Default: `[]`. See: [https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volume-v1-core](https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volume-v1-core)
- **labels** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – Additional labels that should be included in the Job’s Pod. See: [https://kubernetes.io/docs/concepts/overview/working-with-objects/labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels)
- **resources** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – [https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- **scheduler\_name** ( _Optional_ _\[_ _str_ _\]_) – Use a custom Kubernetes scheduler for launched Pods. See: [https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/)
- **load\_incluster\_config** ( _bool_) – Whether the op is running within a k8s cluster. If `True`, we assume the launcher is running within the target cluster and load config using `kubernetes.config.load_incluster_config`. Otherwise, we will use the k8s config specified in `kubeconfig_file` (using `kubernetes.config.load_kube_config`) or fall back to the default kubeconfig. Default: True,
- **kubeconfig\_file** ( _Optional_ _\[_ _str_ _\]_) – The kubeconfig file from which to load config. Defaults to using the default kubeconfig. Default: None.
- **timeout** ( _Optional_ _\[_ _int_ _\]_) – Raise an exception if the op takes longer than this timeout in seconds to execute. Default: None.
- **container\_config** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s pod’s main container ( [https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#container-v1-core](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#container-v1-core)). Keys can either snake\_case or camelCase.Default: None.
- **pod\_template\_spec\_metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s pod’s metadata ( [https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta)). Keys can either snake\_case or camelCase. Default: None.
- **pod\_spec\_config** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s pod’s pod spec ( [https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodSpec](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodSpec)). Keys can either snake\_case or camelCase. Default: None.
- **job\_metadata** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s job’s metadata ( [https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta)). Keys can either snake\_case or camelCase. Default: None.
- **job\_spec\_config** ( _Optional_ _\[_ _Dict_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s job’s job spec ( [https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#jobspec-v1-batch](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.30/#jobspec-v1-batch)). Keys can either snake\_case or camelCase.Default: None.
- **k8s\_job\_name** ( _Optional_ _\[_ _str_ _\]_) – Overrides the name of the k8s job. If not set, will be set to a unique name based on the current run ID and the name of the calling op. If set, make sure that the passed in name is a valid Kubernetes job name that does not already exist in the cluster.
- **merge\_behavior** ( _Optional_ _\[_ _K8sConfigMergeBehavior_ _\]_) – How raw k8s config set on this op should be merged with any raw k8s config set on the code location that launched the op. By default, the value is K8sConfigMergeBehavior.DEEP, meaning that the two dictionaries are recursively merged, appending list fields together and merging dictionary fields. Setting it to SHALLOW will make the dictionaries shallowly merged - any shared values in the dictionaries will be replaced by the values set on this op.
- **delete\_failed\_k8s\_jobs** ( _bool_) – Whether to immediately delete failed Kubernetes jobs. If False, failed jobs will remain accessible through the Kubernetes API until deleted by a user or cleaned up by the .spec.ttlSecondsAfterFinished parameter of the job. ( [https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/)). Defaults to True.

## Python API [​](https://docs.dagster.io/api/libraries/dagster-k8s\#python-api "Direct link to Python API")

The `K8sRunLauncher` allows webserver instances to be configured to launch new runs by starting
per-run Kubernetes Jobs. To configure the `K8sRunLauncher`, your `dagster.yaml` should
include a section like:

```codeBlockLines_e6Vv
run_launcher:
  module: dagster_k8s.launcher
  class: K8sRunLauncher
  config:
    image_pull_secrets:
    service_account_name: dagster
    job_image: "my-company.com/image:latest"
    dagster_home: "/opt/dagster/dagster_home"
    postgres_password_secret: "dagster-postgresql-secret"
    image_pull_policy: "IfNotPresent"
    job_namespace: "dagster"
    instance_config_map: "dagster-instance"
    env_config_maps:
      - "dagster-k8s-job-runner-env"
    env_secrets:
      - "dagster-k8s-some-secret"
    env_vars:
      - "ENV_VAR=1"
    labels:
    resources:
    run_k8s_config:
      pod_template_spec_metadata:
      pod_spec_config:
      job_metadata:
      job_spec_config:
      container_config:
    volume_mounts:
    volumes:
    security_context:
    scheduler_name:
    kubeconfig_file:

```

## Helm chart [​](https://docs.dagster.io/api/libraries/dagster-k8s\#helm-chart "Direct link to Helm chart")

For local dev (e.g., on kind or minikube):

```codeBlockLines_e6Vv
helm install \
    --set dagsterWebserver.image.repository="dagster.io/buildkite-test-image" \
    --set dagsterWebserver.image.tag="py310-latest" \
    --set job_runner.image.repository="dagster.io/buildkite-test-image" \
    --set job_runner.image.tag="py310-latest" \
    --set imagePullPolicy="IfNotPresent" \
    dagster \
    helm/dagster/

```

Upon installation, the Helm chart will provide instructions for port forwarding
the Dagster webserver and Flower (if configured).

## Running tests [​](https://docs.dagster.io/api/libraries/dagster-k8s\#running-tests "Direct link to Running tests")

To run the unit tests:

```codeBlockLines_e6Vv
pytest -m "not integration"

```

To run the integration tests, you must have [Docker](https://docs.docker.com/install/),
[kind](https://kind.sigs.k8s.io/docs/user/quick-start#installation),
and [helm](https://helm.sh/docs/intro/install/) installed.

On macOS:

```codeBlockLines_e6Vv
brew install kind
brew install helm

```

Docker must be running.

You may experience slow first test runs thanks to image pulls (run `pytest -svv --fulltrace` for
visibility). Building images and loading them to the kind cluster is slow, and there is
no visibility into the progress of the load.

**NOTE:** This process is quite slow, as it requires bootstrapping a local `kind` cluster with
Docker images and the `dagster-k8s` Helm chart. For faster development, you can either:

1. Keep a warm kind cluster
2. Use a remote K8s cluster, e.g. via AWS EKS or GCP GKE
Instructions are below.

### Faster local development (with kind) [​](https://docs.dagster.io/api/libraries/dagster-k8s\#faster-local-development-with-kind "Direct link to Faster local development (with kind)")

You may find that the kind cluster creation, image loading, and kind cluster creation loop
is too slow for effective local dev.

You may bypass cluster creation and image loading in the following way. First add the `--no-cleanup`
flag to your pytest invocation:

```codeBlockLines_e6Vv
pytest --no-cleanup -s -vvv -m "not integration"

```

The tests will run as before, but the kind cluster will be left running after the tests are completed.

For subsequent test runs, you can run:

```codeBlockLines_e6Vv
pytest --kind-cluster="cluster-d9971c84d44d47f382a2928c8c161faa" --existing-helm-namespace="dagster-test-95590a" -s -vvv -m "not integration"

```

This will bypass cluster creation, image loading, and Helm chart installation, for much faster tests.

The kind cluster name and Helm namespace for this command can be found in the logs, or retrieved
via the respective CLIs, using `kind get clusters` and `kubectl get namespaces`. Note that
for `kubectl` and `helm` to work correctly with a kind cluster, you should override your
kubeconfig file location with:

```codeBlockLines_e6Vv
kind get kubeconfig --name kind-test > /tmp/kubeconfig
export KUBECONFIG=/tmp/kubeconfig

```

#### Manual kind cluster setup [​](https://docs.dagster.io/api/libraries/dagster-k8s\#manual-kind-cluster-setup "Direct link to Manual kind cluster setup")

The test fixtures provided by `dagster-k8s` automate the process described below, but sometimes
it’s useful to manually configure a kind cluster and load images onto it.

First, ensure you have a Docker image appropriate for your Python version. Run, from the root of
the repo:

```codeBlockLines_e6Vv
./python_modules/dagster-test/dagster_test/test_project/build.sh 3.7.6 \
    dagster.io.priv/buildkite-test-image:py310-latest

```

In the above invocation, the Python majmin version should be appropriate for your desired tests.

Then run the following commands to create the cluster and load the image. Note that there is no
feedback from the loading process.

```codeBlockLines_e6Vv
kind create cluster --name kind-test
kind load docker-image --name kind-test dagster.io/dagster-docker-buildkite:py310-latest

```

If you are deploying the Helm chart with an in-cluster Postgres (rather than an external database),
and/or with dagster-celery workers (and a RabbitMQ), you’ll also want to have images present for
rabbitmq and postgresql:

```codeBlockLines_e6Vv
docker pull docker.io/bitnami/rabbitmq
docker pull docker.io/bitnami/postgresql

kind load docker-image --name kind-test docker.io/bitnami/rabbitmq:latest
kind load docker-image --name kind-test docker.io/bitnami/postgresql:latest

```

Then you can run pytest as follows:

```codeBlockLines_e6Vv
pytest --kind-cluster=kind-test

```

### Faster local development (with an existing K8s cluster) [​](https://docs.dagster.io/api/libraries/dagster-k8s\#faster-local-development-with-an-existing-k8s-cluster "Direct link to Faster local development (with an existing K8s cluster)")

If you already have a development K8s cluster available, you can run tests on that cluster vs.
running locally in `kind`.

For this to work, first build and deploy the test image to a registry available to your cluster.
For example, with a private ECR repository:

```codeBlockLines_e6Vv
./python_modules/dagster-test/dagster_test/test_project/build.sh 3.7.6
docker tag dagster-docker-buildkite:latest $AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com/dagster-k8s-tests:2020-04-21T21-04-06

aws ecr get-login --no-include-email --region us-west-1 | sh
docker push $AWS_ACCOUNT_ID.dkr.ecr.us-west-1.amazonaws.com/dagster-k8s-tests:2020-04-21T21-04-06

```

Then, you can run tests on EKS with:

```codeBlockLines_e6Vv
export DAGSTER_DOCKER_IMAGE_TAG="2020-04-21T21-04-06"
export DAGSTER_DOCKER_REPOSITORY="$AWS_ACCOUNT_ID.dkr.ecr.us-west-2.amazonaws.com"
export DAGSTER_DOCKER_IMAGE="dagster-k8s-tests"

# First run with --no-cleanup to leave Helm chart in place
pytest --cluster-provider="kubeconfig" --no-cleanup -s -vvv

# Subsequent runs against existing Helm chart
pytest --cluster-provider="kubeconfig" --existing-helm-namespace="dagster-test-<some id>" -s -vvv

```

### Validating Helm charts [​](https://docs.dagster.io/api/libraries/dagster-k8s\#validating-helm-charts "Direct link to Validating Helm charts")

To test / validate Helm charts, you can run:

```codeBlockLines_e6Vv
helm install dagster --dry-run --debug helm/dagster
helm lint

```

### Enabling GCR access from Minikube [​](https://docs.dagster.io/api/libraries/dagster-k8s\#enabling-gcr-access-from-minikube "Direct link to Enabling GCR access from Minikube")

To enable GCR access from Minikube:

```codeBlockLines_e6Vv
kubectl create secret docker-registry element-dev-key \
    --docker-server=https://gcr.io \
    --docker-username=oauth2accesstoken \
    --docker-password="$(gcloud auth print-access-token)" \
    --docker-email=my@email.com

```

### A note about PVCs [​](https://docs.dagster.io/api/libraries/dagster-k8s\#a-note-about-pvcs "Direct link to A note about PVCs")

Both the Postgres and the RabbitMQ Helm charts will store credentials using Persistent Volume
Claims, which will outlive test invocations and calls to `helm uninstall`. These must be deleted if
you want to change credentials. To view your pvcs, run:

```codeBlockLines_e6Vv
kubectl get pvc

```

### Testing Redis [​](https://docs.dagster.io/api/libraries/dagster-k8s\#testing-redis "Direct link to Testing Redis")

The Redis Helm chart installs w/ a randomly-generated password by default; turn this off:

```codeBlockLines_e6Vv
helm install dagredis stable/redis --set usePassword=false

```

Then, to connect to your database from outside the cluster execute the following commands:

```codeBlockLines_e6Vv
kubectl port-forward --namespace default svc/dagredis-master 6379:6379
redis-cli -h 127.0.0.1 -p 6379

```

## Pipes [​](https://docs.dagster.io/api/libraries/dagster-k8s\#pipes "Direct link to Pipes")

`class` dagster\_k8s.PipesK8sClient

A pipes client for launching kubernetes pods.

By default context is injected via environment variables and messages are parsed out of
the pod logs, with other logs forwarded to stdout of the orchestration process.

The first container within the containers list of the pod spec is expected (or set) to be
the container prepared for pipes protocol communication.

Parameters:

- **env** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – An optional dict of environment variables to pass to the subprocess.
- **context\_injector** ( _Optional_ _\[_ [_PipesContextInjector_](https://docs.dagster.io/api/dagster/pipes#dagster.PipesContextInjector) _\]_) – A context injector to use to inject context into the k8s container process. Defaults to `PipesEnvContextInjector`.
- **message\_reader** ( _Optional_ _\[_ [_PipesMessageReader_](https://docs.dagster.io/api/dagster/pipes#dagster.PipesMessageReader) _\]_) – A message reader to use to read messages from the k8s container process. Defaults to [`PipesK8sPodLogsMessageReader`](https://docs.dagster.io/api/libraries/dagster-k8s#dagster_k8s.PipesK8sPodLogsMessageReader).
- **load\_incluster\_config** ( _Optional_ _\[_ _bool_ _\]_) – Whether this client is expected to be running from inside a kubernetes cluster and should load config using `kubernetes.config.load_incluster_config`. Otherwise `kubernetes.config.load_kube_config` is used with the kubeconfig\_file argument. Default: None
- **kubeconfig\_file** ( _Optional_ _\[_ _str_ _\]_) – The value to pass as the config\_file argument to `kubernetes.config.load_kube_config`. Default: None.
- **kube\_context** ( _Optional_ _\[_ _str_ _\]_) – The value to pass as the context argument to `kubernetes.config.load_kube_config`. Default: None.
- **poll\_interval** ( _Optional_ _\[_ _float_ _\]_) – How many seconds to wait between requests when polling the kubernetes API Default: 10.

run

Publish a kubernetes pod and wait for it to complete, enriched with the pipes protocol.

Parameters:

- **context** ( _Union_ _\[_ [_OpExecutionContext_](https://docs.dagster.io/api/dagster/execution#dagster.OpExecutionContext) _,_ [_AssetExecutionContext_](https://docs.dagster.io/api/dagster/execution#dagster.AssetExecutionContext) _\]_) – The execution context.
- **image** ( _Optional_ _\[_ _str_ _\]_) – The image to set the first container in the pod spec to use.
- **command** ( _Optional_ _\[_ _Union_ _\[_ _str_ _,_ _Sequence_ _\[_ _str_ _\]_ _\]_ _\]_) – The command to set the first container in the pod spec to use.
- **namespace** ( _Optional_ _\[_ _str_ _\]_) – Which kubernetes namespace to use, defaults to the current namespace if running inside a kubernetes cluster or falling back to “default”.
- **env** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _str_ _\]_ _\]_) – A mapping of environment variable names to values to set on the first container in the pod spec, on top of those configured on resource.
- **base\_pod\_meta** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s pod’s metadata ( [https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/object-meta/#ObjectMeta)) Keys can either snake\_case or camelCase. The name value will be overridden.
- **base\_pod\_spec** ( _Optional_ _\[_ _Mapping_ _\[_ _str_ _,_ _Any_ _\]_ _\]_) – Raw k8s config for the k8s pod’s pod spec ( [https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodSpec](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#PodSpec)). Keys can either snake\_case or camelCase. The dagster context will be readable from any container within the pod, but only the first container in the pod.spec.containers will be able to communicate back to Dagster.
- **extras** ( _Optional_ _\[_ _PipesExtras_ _\]_) – Extra values to pass along as part of the ext protocol.
- **context\_injector** ( _Optional_ _\[_ [_PipesContextInjector_](https://docs.dagster.io/api/dagster/pipes#dagster.PipesContextInjector) _\]_) – Override the default ext protocol context injection.
- **message\_reader** ( _Optional_ _\[_ [_PipesMessageReader_](https://docs.dagster.io/api/dagster/pipes#dagster.PipesMessageReader) _\]_) – Override the default ext protocol message reader.
- **ignore\_containers** ( _Optional_ _\[_ _Set_ _\]_) – Ignore certain containers from waiting for termination. Defaults to None.
- **enable\_multi\_container\_logs** ( _bool_) – Whether or not to enable multi-container log consumption.

Returns:
Wrapper containing results reported by the external
process.

Return type: PipesClientCompletedInvocation

`class` dagster\_k8s.PipesK8sPodLogsMessageReader

Message reader that reads messages from kubernetes pod logs.

- [APIs](https://docs.dagster.io/api/libraries/dagster-k8s#apis)
- [Ops](https://docs.dagster.io/api/libraries/dagster-k8s#ops)
- [Python API](https://docs.dagster.io/api/libraries/dagster-k8s#python-api)
- [Helm chart](https://docs.dagster.io/api/libraries/dagster-k8s#helm-chart)
- [Running tests](https://docs.dagster.io/api/libraries/dagster-k8s#running-tests)
  - [Faster local development (with kind)](https://docs.dagster.io/api/libraries/dagster-k8s#faster-local-development-with-kind)
    - [Manual kind cluster setup](https://docs.dagster.io/api/libraries/dagster-k8s#manual-kind-cluster-setup)
  - [Faster local development (with an existing K8s cluster)](https://docs.dagster.io/api/libraries/dagster-k8s#faster-local-development-with-an-existing-k8s-cluster)
  - [Validating Helm charts](https://docs.dagster.io/api/libraries/dagster-k8s#validating-helm-charts)
  - [Enabling GCR access from Minikube](https://docs.dagster.io/api/libraries/dagster-k8s#enabling-gcr-access-from-minikube)
  - [A note about PVCs](https://docs.dagster.io/api/libraries/dagster-k8s#a-note-about-pvcs)
  - [Testing Redis](https://docs.dagster.io/api/libraries/dagster-k8s#testing-redis)
- [Pipes](https://docs.dagster.io/api/libraries/dagster-k8s#pipes)
