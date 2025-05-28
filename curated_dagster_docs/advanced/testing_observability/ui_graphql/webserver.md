
On this page

The Dagster webserver serves the Dagster UI, a web-based interface for viewing and interacting with Dagster objects. It also responds to GraphQL queries.

In the UI, you can inspect Dagster objects (ex: assets, jobs, schedules), launch runs, view launched runs, and view assets produced by those runs.

## Launching the webserver [​](https://docs.dagster.io/guides/operate/webserver\#launching-the-webserver "Direct link to Launching the webserver")

The easiest way to launch the webserver from the command line during local development is to run:

```codeBlockLines_e6Vv
dagster dev

```

This command launches both the Dagster webserver and the [Dagster daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon), allowing you to start a full local deployment of Dagster from the command line.

The command will print out the URL you can access the UI from in the browser, usually on port 3000.

When invoked, the webserver will fetch definitions - such as assets, jobs, schedules, sensors, and resources - from a [`Definitions`](https://docs.dagster.io/api/dagster/definitions#dagster.Definitions) object in a Python module or package or the code locations configured in an open source deployment's [workspace files](https://docs.dagster.io/guides/deploy/code-locations/workspace-yaml). For more information, see the [code locations documentation](https://docs.dagster.io/guides/deploy/code-locations/).

You can also launch the webserver by itself from the command line by running:

```codeBlockLines_e6Vv
dagster-webserver

```

Note that several Dagster features, like schedules and sensors, require the Dagster daemon to be running in order to function.

## Dagster UI reference [​](https://docs.dagster.io/guides/operate/webserver\#dagster-ui-reference "Direct link to Dagster UI reference")

### Overview page [​](https://docs.dagster.io/guides/operate/webserver\#overview-page "Direct link to Overview page")

- **Description**: This page, also known as the "factory floor", provides a high-level look at the activity in your Dagster deployment, across all code locations. This includes information about runs, jobs, schedules, sensors, resources, and backfills, all of which can be accessed using the tabs on this page.

- **Accessed by**: Clicking **Overview** in the top navigation bar


![The Overview tab, also known as the Factory Floor, in the Dagster UI](https://docs.dagster.io/assets/images/factory-floor-7eb8447fdb3081b7c8a7428030cc2cba.png)

### Assets [​](https://docs.dagster.io/guides/operate/webserver\#assets "Direct link to Assets")

- Asset catalog
- Asset catalog (Dagster+ Pro)
- Catalog views (Dagster+)
- Global asset lineage
- Asset details

**Asset catalog (OSS)**

- **Description**: The **Asset catalog** page lists all [assets](https://docs.dagster.io/guides/build/assets/) in your Dagster deployment, which can be filtered by asset key, compute kind, asset group, [code location](https://docs.dagster.io/guides/deploy/code-locations/), and [tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags/#tags). Clicking an asset opens the **Asset details** page for that asset. You can also navigate to the **Global asset lineage** page, reload definitions, and materialize assets.

- **Accessed by:** Clicking **Assets** in the top navigation bar


![The Asset Catalog page in the Dagster UI](https://docs.dagster.io/assets/images/asset-catalog-5de66192cbe70fd496ef1f847f49a3cc.png)

**Asset catalog (Dagster+ Pro)**

note

This feature is only available in Dagster+ Pro.

- **Description**: This version of the **Asset catalog** page includes all the information and functionality of the original page, broken out by compute kind, asset group, [code location](https://docs.dagster.io/guides/deploy/code-locations/), [tags](https://docs.dagster.io/guides/build/assets/metadata-and-tags/#tags), and [owners](https://docs.dagster.io/guides/build/assets/metadata-and-tags/#owners), etc. On this page, you can:
  - View all [assets](https://docs.dagster.io/guides/build/assets/) in your Dagster deployment
  - View details about a specific asset by clicking on it
  - Search assets by asset key, compute kind, asset group, code location, tags, owners, etc.
  - Access the global asset lineage
  - Reload definitions
- **Accessed by:** Clicking **Catalog** in the top navigation


![The Asset Catalog page in the Dagster UI](https://docs.dagster.io/assets/images/asset-catalog-cloud-pro-17f5b7009f3b0936a0ebf3568e257a3a.png)

**Catalog views (Dagster+)**

- **Description**: **Catalog views** save a set of filters against the **Asset catalog** to show only the assets you want to see. You can share these views for easy access and faster team collaboration. With **Catalog views**, you can:
  - Filter for a scoped set of [assets](https://docs.dagster.io/guides/build/assets) in your Dagster deployment
  - Create shared views of assets for easier team collaboration
- **Accessed by:**
  - Clicking **Catalog** in the top navigation
  - **From the Global asset lineage**: Clicking **View global asset lineage**, located near the top right corner of the **Catalog** page

![The Catalog views dropdown in the Dagster+ Pro Catalog UI](https://docs.dagster.io/assets/images/catalog-views-67be2d8845e7343ec6426c6f492b14ce.png)

**Global asset lineage**

- **Description**: The **Global asset lineage** page displays dependencies between all of the assets in your Dagster deployment, across all code locations. On this page, you can:
  - Filter assets by group
  - Filter a subset of assets by using [asset selection syntax](https://docs.dagster.io/guides/build/assets/asset-selection-syntax)
  - Reload definitions
  - Materialize all or a selection of assets
  - View run details for the latest materialization of any asset
- **Accessed by**:
  - **From the Asset catalog**: Clicking **View global asset lineage**, located near the top right corner of the page
  - **From the Asset details page**: Clicking the **Lineage tab**

![The Global asset lineage page in the Dagster UI](https://docs.dagster.io/assets/images/global-asset-lineage-ec6b4c574961906b8f58a3c3dc12c49f.png)

**Asset details**

- **Description**: The **Asset details** page contains details about a single asset. Use the tabs on this page to view detailed information about the asset:
  - **Overview** \- Information about the asset such as its description, resources, config, type, etc.
  - **Partitions** \- The asset's partitions, including their materialization status, metadata, and run information
  - **Events** \- The asset's materialization history
  - **Checks** \- The [Asset checks](https://docs.dagster.io/guides/test/asset-checks) defined for the asset
  - **Lineage** \- The asset's lineage in the **Global asset lineage** page
  - **Automation** \- The [Declarative Automation conditions](https://docs.dagster.io/guides/automate/declarative-automation) associated with the asset
  - **Insights** \- **Dagster+ only.** Historical information about the asset, such as failures and credit usage. Refer to the [Dagster+ Insights](https://docs.dagster.io/dagster-plus/features/insights/) documentation for more information.
- **Accessed by**: Clicking an asset in the **Asset catalog**


![The Asset Details page in the Dagster UI](https://docs.dagster.io/assets/images/asset-details-6a5ebf1f6d56025838fb6301e9813e0f.png)

### Runs [​](https://docs.dagster.io/guides/operate/webserver\#runs "Direct link to Runs")

- All runs
- Run details
- Run logs

**All runs**

- **Description**: The **Runs** page lists all job runs, which can be filtered by job name, run ID, execution status, or tag. Click a run ID to open the **Run details** page and view details for that run.

- **Accessed by**: Clicking **Runs** in the top navigation bar


![UI Runs page](https://docs.dagster.io/assets/images/runs-page-61078d5071ecd70928f02894c3ec52f7.png)

**Run details**

- **Description**: The **Run details** page contains details about a single run, including timing information, errors, and logs. The upper left pane contains a Gantt chart, indicating how long each asset or op took to execute. The bottom pane displays filterable events and logs emitted during execution.

In this page, you can:
  - **View structured event and raw compute logs.** Refer to the run logs tab for more info.
  - **Re-execute a run** using the same configuration by clicking the **Re-execute** button. Related runs (e.g., runs created by re-executing the same previous run) are grouped in the right pane for easy reference
- **Accessed by**: Clicking a run in the **Run details** page


![UI Run details page](https://docs.dagster.io/assets/images/run-details-c4808224aed98c5e9e7e2fd68a583efc.png)

**Run logs**

- **Description**: Located at the bottom of the **Run details** page, the run logs list every event that occurred in a run, the type of event, and detailed information about the event itself. There are two types of logs, which we'll discuss in the next section:
  - Structured event logs
  - Raw compute logs
- **Accessed by**: Scrolling to the bottom of the **Run details** page


**Structured event logs**

- **Description**: Structured logs are enriched and categorized with metadata. For example, a label of which asset a log is about, links to an asset’s metadata, and what type of event it is available. This structuring also enables easier filtering and searching in the logs.

- **Accessed by**: Clicking the **left side** of the toggle next to the log filter field


![Structured event logs in the Run details page](https://docs.dagster.io/assets/images/run-details-event-logs-31cbf2e1ff5927546ae05361a0356a6f.png)

**Raw compute logs**

- **Description**: The raw compute logs contain logs for both [`stdout` and `stderr`](https://stackoverflow.com/questions/3385201/confused-about-stdin-stdout-and-stderr), which you can toggle between. To download the logs, click the **arrow icon** near the top right corner of the logs.

- **Accessed by**: Clicking the **right side** of the toggle next to the log filter field


![Raw compute logs in the Run details page](https://docs.dagster.io/assets/images/run-details-compute-logs-d853e13019b7a4cb867918b3ead99b38.png)

### Schedules [​](https://docs.dagster.io/guides/operate/webserver\#schedules "Direct link to Schedules")

- All schedules
- Schedule details

**All schedules**

- **Description**: The **Schedules** page lists all [schedules](https://docs.dagster.io/guides/automate/schedules) defined in your Dagster deployment, as well as information about upcoming ticks for anticipated scheduled runs. Click a schedule to open the **Schedule details** page.

- **Accessed by**: Clicking **Overview (top nav) > Schedules tab**


![UI Schedules page](https://docs.dagster.io/assets/images/schedules-tab-29661f359ffd0b9242274c9a4cc3172a.png)

**Schedule details**

- **Description**: The **Schedule details** page contains details about a single schedule, including its next tick, tick history, and run history. Clicking the **Preview tick result** button near the top right corner of the page allows you to test the schedule.

- **Accessed by**: Clicking a schedule in the **Schedules** page.


![UI Schedule details page](https://docs.dagster.io/assets/images/schedule-details-a966bc120bed7ddf8c4ac8a9f7a9ec92.png)

### Sensors [​](https://docs.dagster.io/guides/operate/webserver\#sensors "Direct link to Sensors")

- All sensors
- Sensor details

**All sensors**

- **Description**: The **Sensors** page lists all [sensors](https://docs.dagster.io/guides/automate/sensors) defined in your Dagster deployment, as well as information about the sensor's frequency and its last tick. Click a sensor to view details about the sensor, including its recent tick history and recent runs.

- **Accessed by**: Clicking **Overview (top nav) > Sensors tab**


![UI Sensors page](https://docs.dagster.io/assets/images/sensors-tab-3ef5551af90b2934cb05eaf48ea2975c.png)

**Sensor details**

- **Description**: The **Sensor details** page contains details about a single sensor, including its next tick, tick history, and run history. Clicking the **Preview tick result** button near the top right corner of the page allows you to test the sensor.

- **Accessed by**: Clicking a sensor in the **Sensors** page


![UI Sensor details page](https://docs.dagster.io/assets/images/sensor-details-ba3e90cd3577ddf0dfd030607a3a1954.png)

### Resources [​](https://docs.dagster.io/guides/operate/webserver\#resources "Direct link to Resources")

- All resources
- Resource details

**All resources**

- **Description**: The **Resources** page lists all [resources](https://docs.dagster.io/guides/build/external-resources/) defined in your Dagster deployment, across all code locations. Clicking a resource will open the **Resource details** page.

- **Accessed by**: Clicking **Overview (top nav) > Resources tab**


![UI Resources page](https://docs.dagster.io/assets/images/resources-tab-cf1b65b977202b86f31a596aad28e537.png)

**Resource details**

- **Description**: The **Resource details** page contains detailed information about a resource, including its configuration, description, and uses. Click the tabs below for more information about the tabs on this page.

- **Accessed by**: Clicking a resource in the **Resources** page.


- Configuration tab
- Uses tab

**Configuration tab**

- **Description**: The **Configuration** tab contains detailed information about a resource's configuration, including the name of each key, type, and value of each config value. If a key's value is an [environment variable](https://docs.dagster.io/guides/deploy/using-environment-variables-and-secrets), an `Env var` badge will display next to the value.

- **Accessed by**: On the **Resource details** page, clicking the **Configuration tab**


![UI Resource details - Configuration tab](https://docs.dagster.io/assets/images/resource-details-configuration-tab-10eecee384b2661d29a9adb6090844b7.png)

**Uses tab**

- **Description**: The **Uses** tab contains information about the other Dagster definitions that use the resource, including [assets](https://docs.dagster.io/guides/build/assets/), [jobs](https://docs.dagster.io/guides/build/jobs/), and [ops](https://docs.dagster.io/guides/build/ops/). Clicking on any of these definitions will open the details page for that definition type.

- **Accessed by**: On the \*\*Resource details\* page, clicking the \*\*Uses tab\*\*


![UI Resource details - Uses tab](https://docs.dagster.io/assets/images/resource-details-uses-tab-083032b6d0756c28e7a6979062c2fd87.png)

### Backfills [​](https://docs.dagster.io/guides/operate/webserver\#backfills "Direct link to Backfills")

- **Description**: The **Backfills** tab contains information about the backfills in your Dagster deployment, across all code locations. It includes information about when the partition was created, its target, status, run status, and more.

- **Accessed by**: Clicking **Overview (top nav) > Backfills tab**


![UI Backfills tab](https://docs.dagster.io/assets/images/backfills-tab-b3ac9d2ad492162ca6c63bf2c065d707.png)

### Jobs [​](https://docs.dagster.io/guides/operate/webserver\#jobs "Direct link to Jobs")

- All jobs
- Job details

**All jobs**

- **Description**: The **Jobs** page lists all [jobs](https://docs.dagster.io/guides/build/jobs/) defined in your Dagster deployment across all code locations. It includes information about the job's schedule or sensor, its latest run time, and its history. Click a job to open the **Job details** page.

- **Accessed by**: Clicking **Overview (top nav) > Jobs tab**


![UI Job Definition](https://docs.dagster.io/assets/images/jobs-tab-5133fa8f86f7a621f45c32103bacae6e.png)

**Job details**

- **Description**: The **Job details** page contains detailed information about a job. Click the tabs below for more information about the tabs on this page.

- **Accessed by**: Clicking a job in the **Jobs** page.


- Overview tab
- Launchpad tab
- Runs tab
- Partitions tab

**Overview tab**

- **Description**: The **Overview** tab in the **Job details** page shows the graph of assets and/or ops that make up a job.

- **Accessed by:** On the **Job details** page, clicking the **Overview** tab


![UI Job Definition](https://docs.dagster.io/assets/images/job-definition-with-ops-6ef8e47dad9ccc6748efb2adf3a472f1.png)

**Launchpad tab**

- **Description**: The **Launchpad tab** provides a configuration editor to let you experiment with configuration and launch runs. **Note**: For assets, this tab will only display if a job requires config. It displays by default for all op jobs.

- **Accessed by:** On the **Job details** page, clicking the **Launchpad** tab


![UI Launchpad](https://docs.dagster.io/assets/images/job-config-with-ops-024d1a50df7414a77022b68428ba00e2.png)

**Runs tab**

- **Description**: The **Runs** tab displays a list of recent runs for a job. Clicking a run will open the \[ **Run details** page.\
\
- **Accessed by:** On the **Job details** page, clicking the **Runs** tab\
\
\
![UI Job runs tab](https://docs.dagster.io/assets/images/jobs-runs-tab-e15e342e1997a97bfd5e320d0e6a2d8a.png)\
\
**Partitions tab**\
\
- **Description**: The **Partitions** tab displays information about the [partitions](https://docs.dagster.io/guides/build/partitions-and-backfills) associated with the job, including the total number of partitions, the number of missing partitions, and the job's backfill history. **Note**: This tab will display only if the job contains partitions.\
\
- **Accessed by:** On the **Job details** page, clicking the **Partitions** tab\
\
\
![UI Job Partitions tab](https://docs.dagster.io/assets/images/jobs-partitions-tab-638f1f15e46bbbd4de34406626d4921a.png)\
\
### Deployment [​](https://docs.dagster.io/guides/operate/webserver\#deployment "Direct link to Deployment")\
\
The **Deployment** page includes information about the status of the code locations in your Dagster deployment, daemon (Open Source) or agent (Cloud) health, schedules, sensors, and configuration details.\
\
- Code locations tab\
- Open Source (OSS)\
- Dagster+\
\
**Code locations tab**\
\
- **Description**: The **Code locations** tab contains information about the code locations in your Dagster deployment, including their current status, when they were last updated, and high-level details about the definitions they contain. You can reload Dagster definitions by:\
  - Clicking **Reload all** to reload all definitions in all code locations\
  - Clicking **Reload** next to a specific code location to reload only that code location's definitions\
- **Accessed by**:\
  - Clicking **Deployment** in the top navigation bar\
  - On the **Deployment overview** page, clicking the **Code locations** tab\
\
![UI Deployment overview page](https://docs.dagster.io/assets/images/deployment-code-locations-3ab5612d6413ee5ba2d8ea27052e66b8.png)\
\
**Open Source (OSS)**\
\
In addition to the **Code locations** tab, Dagster OSS deployments contain a few additional tabs. Click the tabs below for more information.\
\
- Daemons tab\
- Configuration tab\
\
**Daemons tab**\
\
- **Description**: The **Daemons** tab contains information about the [daemons](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) in an Open Source Dagster deployment, including their current status and when their last heartbeat was detected.\
- **Accessed by**: On the **Deployment overview** page, clicking the **Daemons** tab\
\
![UI Deployment - Daemons tab](https://docs.dagster.io/assets/images/deployment-daemons-tab-4ba1c99dba12beec2997a71ed8c9f33f.png)\
\
**Configuration tab**\
\
- **Description**: The **Configuration** tab displays information about the configuration for a Dagster deployment, which is managed through the [`dagster.yaml`](https://docs.dagster.io/guides/deploy/dagster-yaml) file\
- **Accessed by**: On the **Deployment overview** page, clicking the **Configuration** tab\
\
![UI Deployment - Configuration tab](https://docs.dagster.io/assets/images/deployment-configuration-tab-8ab5d7337686f3d05920be6109856171.png)\
\
**Dagster+**\
\
In addition to the **Code locations** tab, Dagster+ deployments contain a few additional tabs. Click the tabs below for more information.\
\
- Agents tab\
- Environmental variables tab\
- Alerts tab\
\
**Agents tab**\
\
- **Description**: The **Agents** tab contains information about the agents in a Dagster+ deployment.\
- **Accessed by**: On the **Deployment overview** page, clicking the **Agents** tab\
\
![UI Dagster+ Deployment - Agents tab](https://docs.dagster.io/assets/images/deployment-cloud-agents-tab-97c849412dfc541b9abe49adfce8a39d.png)\
\
**Environment variables tab**\
\
- **Description**: The **Agents** tab contains information about the environment variables configured in a Dagster+ deployment. Refer to the [Dagster+ environment variables documentation](https://docs.dagster.io/dagster-plus/deployment/management/environment-variables/) for more info.\
- **Accessed by**: On the **Deployment overview** page, clicking the **Environment variables** tab\
\
![UI Cloud Deployment - Environment variables tab](https://docs.dagster.io/assets/images/deployment-cloud-environment-variables-tab-b4747f19cddfe2bd3e0e1ea450625bc9.png)\
\
**Alerts tab**\
\
- **Description**: The **Alerts** tab contains information about the alert policies configured for a Dagster+ deployment. Refer to the [Dagster+ alerts guide](https://docs.dagster.io/dagster-plus/features/alerts/) for more info.\
- **Accessed by**: On the **Deployment overview** page, clicking the **Alerts** tab\
\
![UI Dagster+ Deployment - Alerts tab](https://docs.dagster.io/assets/images/deployment-cloud-alerts-tab-167339c2dbf784298ab3d6fca3d9fa6f.png)\
\
- [Launching the webserver](https://docs.dagster.io/guides/operate/webserver#launching-the-webserver)\
- [Dagster UI reference](https://docs.dagster.io/guides/operate/webserver#dagster-ui-reference)\
  - [Overview page](https://docs.dagster.io/guides/operate/webserver#overview-page)\
  - [Assets](https://docs.dagster.io/guides/operate/webserver#assets)\
  - [Runs](https://docs.dagster.io/guides/operate/webserver#runs)\
  - [Schedules](https://docs.dagster.io/guides/operate/webserver#schedules)\
  - [Sensors](https://docs.dagster.io/guides/operate/webserver#sensors)\
  - [Resources](https://docs.dagster.io/guides/operate/webserver#resources)\
  - [Backfills](https://docs.dagster.io/guides/operate/webserver#backfills)\
  - [Jobs](https://docs.dagster.io/guides/operate/webserver#jobs)\
  - [Deployment](https://docs.dagster.io/guides/operate/webserver#deployment)\
\
