
On this page

This guide provides instructions for deploying Dagster using Docker Compose. This is useful when you want to, for example, deploy Dagster on an AWS EC2 host. A typical Dagster Docker deployment includes a several long-running containers: one for the webserver, one for the daemon, and one for each code location. It also typically executes each run in its own container.

The [full example is available on GitHub](https://github.com/dagster-io/dagster/blob/master/examples/deploy_docker).

Details

Prerequisites\- Familiarity with Docker and Docker Compose - Familiarity with `dagster.yaml`
instance configuration - Familiarity with `workspace.yaml` code location configuration

## Define a Docker image for the Dagster webserver and daemon [​](https://docs.dagster.io/guides/deploy/deployment-options/docker\#define-a-docker-image-for-the-dagster-webserver-and-daemon "Direct link to Define a Docker image for the Dagster webserver and daemon")

The Dagster webserver and daemon are the two _host processes_ in a Dagster deployment. They typically each run in their own container, using the same Docker image. This image contains Dagster packages and configuration, but no user code.

To build this Docker image, use a Dockerfile like the following, with a name like `Dockerfile_dagster`:

```codeBlockLines_e6Vv
FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker

# Set $DAGSTER_HOME and copy dagster.yaml and workspace.yaml there
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME

```

Additionally, the following files should be in the same directory as the Docker file:

- A `workspace.yaml` to tell the webserver and daemon the location of the code servers
- A `dagster.yaml` to configure the Dagster instance

## Define a Docker image for each code location [​](https://docs.dagster.io/guides/deploy/deployment-options/docker\#define-a-docker-image-for-each-code-location "Direct link to Define a Docker image for each code location")

Each code location typically has its own Docker image, and that image is also used for runs launched for that code location.

To build a Docker image for a code location, use a Dockerfile like the following, with a name like `Dockerfile_code_location_1`:

```codeBlockLines_e6Vv
FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

# Add code location code
WORKDIR /opt/dagster/app
COPY directory/with/your/code/ /opt/dagster/app

# Run dagster code server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "definitions.py"]

```

## Write a Docker Compose file [​](https://docs.dagster.io/guides/deploy/deployment-options/docker\#write-a-docker-compose-file "Direct link to Write a Docker Compose file")

The following `docker-compose.yaml` defines how to run the webserver container, daemon container, code location containers, and database container:

docker-compose.yaml

```codeBlockLines_e6Vv
version: '3.7'

services:
  # This service runs the postgres DB used by dagster for run storage, schedule storage,
  # and event log storage. Depending on the hardware you run this Compose on, you may be able
  # to reduce the interval and timeout in the healthcheck to speed up your `docker-compose up` times.
  docker_example_postgresql:
    image: postgres:11
    container_name: docker_example_postgresql
    environment:
      POSTGRES_USER: 'postgres_user'
      POSTGRES_PASSWORD: 'postgres_password'
      POSTGRES_DB: 'postgres_db'
    networks:
      - docker_example_network
    healthcheck:
      test: ['CMD-SHELL', 'pg_isready -U postgres_user -d postgres_db']
      interval: 10s
      timeout: 8s
      retries: 5

  # This service runs the gRPC server that loads your user code, in both dagster-webserver
  # and dagster-daemon. By setting DAGSTER_CURRENT_IMAGE to its own image, we tell the
  # run launcher to use this same image when launching runs in a new container as well.
  # Multiple containers like this can be deployed separately - each just needs to run on
  # its own port, and have its own entry in the workspace.yaml file that's loaded by the
  # webserver.
  docker_example_user_code:
    build:
      context: .
      dockerfile: ./Dockerfile_user_code
    container_name: docker_example_user_code
    image: docker_example_user_code_image
    restart: always
    environment:
      DAGSTER_POSTGRES_USER: 'postgres_user'
      DAGSTER_POSTGRES_PASSWORD: 'postgres_password'
      DAGSTER_POSTGRES_DB: 'postgres_db'
      DAGSTER_CURRENT_IMAGE: 'docker_example_user_code_image'
    networks:
      - docker_example_network

  # This service runs dagster-webserver, which loads your user code from the user code container.
  # Since our instance uses the QueuedRunCoordinator, any runs submitted from the webserver will be put on
  # a queue and later dequeued and launched by dagster-daemon.
  docker_example_webserver:
    build:
      context: .
      dockerfile: ./Dockerfile_dagster
    entrypoint:
      - dagster-webserver
      - -h
      - '0.0.0.0'
      - -p
      - '3000'
      - -w
      - workspace.yaml
    container_name: docker_example_webserver
    expose:
      - '3000'
    ports:
      - '3000:3000'
    environment:
      DAGSTER_POSTGRES_USER: 'postgres_user'
      DAGSTER_POSTGRES_PASSWORD: 'postgres_password'
      DAGSTER_POSTGRES_DB: 'postgres_db'
    volumes: # Make docker client accessible so we can terminate containers from the webserver
      - /var/run/docker.sock:/var/run/docker.sock
      - /tmp/io_manager_storage:/tmp/io_manager_storage
    networks:
      - docker_example_network
    depends_on:
      docker_example_postgresql:
        condition: service_healthy
      docker_example_user_code:
        condition: service_started

  # This service runs the dagster-daemon process, which is responsible for taking runs
  # off of the queue and launching them, as well as creating runs from schedules or sensors.
  docker_example_daemon:
    build:
      context: .
      dockerfile: ./Dockerfile_dagster
    entrypoint:
      - dagster-daemon
      - run
    container_name: docker_example_daemon
    restart: on-failure
    environment:
      DAGSTER_POSTGRES_USER: 'postgres_user'
      DAGSTER_POSTGRES_PASSWORD: 'postgres_password'
      DAGSTER_POSTGRES_DB: 'postgres_db'
    volumes: # Make docker client accessible so we can launch containers using host docker
      - /var/run/docker.sock:/var/run/docker.sock
      - /tmp/io_manager_storage:/tmp/io_manager_storage
    networks:
      - docker_example_network
    depends_on:
      docker_example_postgresql:
        condition: service_healthy
      docker_example_user_code:
        condition: service_started

networks:
  docker_example_network:
    driver: bridge
    name: docker_example_network

```

## Start your deployment [​](https://docs.dagster.io/guides/deploy/deployment-options/docker\#start-your-deployment "Direct link to Start your deployment")

To start the deployment, run:

```codeBlockLines_e6Vv
docker compose up

```

- [Define a Docker image for the Dagster webserver and daemon](https://docs.dagster.io/guides/deploy/deployment-options/docker#define-a-docker-image-for-the-dagster-webserver-and-daemon)
- [Define a Docker image for each code location](https://docs.dagster.io/guides/deploy/deployment-options/docker#define-a-docker-image-for-each-code-location)
- [Write a Docker Compose file](https://docs.dagster.io/guides/deploy/deployment-options/docker#write-a-docker-compose-file)
- [Start your deployment](https://docs.dagster.io/guides/deploy/deployment-options/docker#start-your-deployment)
