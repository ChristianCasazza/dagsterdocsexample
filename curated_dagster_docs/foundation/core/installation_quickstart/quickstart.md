
On this page

Welcome to Dagster! In this guide, you'll use Dagster to create a basic pipeline that:

- Extracts data from a CSV file
- Transforms the data
- Loads the transformed data to a new CSV file

## What you'll learn [​](https://docs.dagster.io/getting-started/quickstart\#what-youll-learn "Direct link to What you'll learn")

- How to set up a basic Dagster project
- How to create a single Dagster asset that encapsulates the entire Extract, Transform, and Load (ETL) process
- How to use Dagster's UI to monitor and execute your pipeline

## Prerequisites [​](https://docs.dagster.io/getting-started/quickstart\#prerequisites "Direct link to Prerequisites")

Prerequisites

To follow the steps in this guide, you'll need:

- Basic Python knowledge
- Python 3.9+ installed on your system. Refer to the [Installation guide](https://docs.dagster.io/getting-started/installation) for information.

## Step 1: Set up the Dagster environment [​](https://docs.dagster.io/getting-started/quickstart\#step-1-set-up-the-dagster-environment "Direct link to Step 1: Set up the Dagster environment")

1. Open the terminal and create a new directory for your project:





```codeBlockLines_e6Vv
mkdir dagster-quickstart
cd dagster-quickstart

```

2. Create and activate a virtual environment:



- MacOS
- Windows

`bash python -m venv venv source venv/bin/activate `

`bash python -m venv venv source venv\Scripts\activate `

3. Install Dagster and the required dependencies:





```codeBlockLines_e6Vv
pip install dagster dagster-webserver pandas

```


## Step 2: Create the Dagster project structure [​](https://docs.dagster.io/getting-started/quickstart\#step-2-create-the-dagster-project-structure "Direct link to Step 2: Create the Dagster project structure")

info

The project structure in this guide is simplified to allow you to get started quickly. When creating new projects, use `dagster project scaffold` to generate a complete Dagster project.

Next, you'll create a basic Dagster project that looks like this:

```codeBlockLines_e6Vv
dagster-quickstart/
├── quickstart/
│   ├── __init__.py
│   └── assets.py
├── data/
    └── sample_data.csv

```

1. To create the files and directories outlined above, run the following:





```codeBlockLines_e6Vv
mkdir quickstart data
touch quickstart/__init__.py quickstart/assets.py
touch data/sample_data.csv

```

2. In the `data/sample_data.csv` file, add the following content:





```codeBlockLines_e6Vv
id,name,age,city
1,Alice,28,New York
2,Bob,35,San Francisco
3,Charlie,42,Chicago
4,Diana,31,Los Angeles

```









This CSV will act as the data source for your Dagster pipeline.


## Step 3: Define the assets [​](https://docs.dagster.io/getting-started/quickstart\#step-3-define-the-assets "Direct link to Step 3: Define the assets")

Now, create the assets for the ETL pipeline. Open `quickstart/assets.py` and add the following code:

```codeBlockLines_e6Vv
import pandas as pd

import dagster as dg

@dg.asset
def processed_data():
    ## Read data from the CSV
    df = pd.read_csv("data/sample_data.csv")

    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv("data/processed_data.csv", index=False)
    return "Data loaded successfully"

## Tell Dagster about the assets that make up the pipeline by
## passing it to the Definitions object
## This allows Dagster to manage the assets' execution and dependencies
defs = dg.Definitions(assets=[processed_data])

```

This may seem unusual if you're used to task-based orchestration. In that case, you'd have three separate steps for extracting, transforming, and loading.

However, in Dagster, you'll model your pipelines using assets as the fundamental building block, rather than tasks.

## Step 4: Run the pipeline [​](https://docs.dagster.io/getting-started/quickstart\#step-4-run-the-pipeline "Direct link to Step 4: Run the pipeline")

1. In the terminal, navigate to your project's root directory and run:





```codeBlockLines_e6Vv
dagster dev -f quickstart/assets.py

```

2. Open your web browser and navigate to `http://localhost:3000`, where you should see the Dagster UI:

![2048 resolution](https://docs.dagster.io/assets/images/dagster-ui-start-d9e349c934143f3dd6436f961f049c0b.png)

3. In the top navigation, click **Assets > View global asset lineage**.

4. Click **Materialize** to run the pipeline.

5. In the popup that displays, click **View**. This will open the **Run details** page, allowing you to view the run as it executes.

![2048 resolution](https://docs.dagster.io/assets/images/run-details-afce3b782860e6c3d60c9a61cb7ebbeb.png)

Use the **view buttons** in near the top left corner of the page to change how the run is displayed. You can also click the asset to view logs and metadata.


## Step 5: Verify the results [​](https://docs.dagster.io/getting-started/quickstart\#step-5-verify-the-results "Direct link to Step 5: Verify the results")

In your terminal, run:

```codeBlockLines_e6Vv
cat data/processed_data.csv

```

You should see the transformed data, including the new `age_group` column:

```codeBlockLines_e6Vv
id,name,age,city,age_group
1,Alice,28,New York,Young
2,Bob,35,San Francisco,Middle
3,Charlie,42,Chicago,Senior
4,Diana,31,Los Angeles,Middle

```

## Next steps [​](https://docs.dagster.io/getting-started/quickstart\#next-steps "Direct link to Next steps")

Congratulations! You've just built and run your first pipeline with Dagster. Next, you can:

- Continue with the [ETL pipeline tutorial](https://docs.dagster.io/etl-pipeline-tutorial/) to learn how to build a more complex ETL pipeline
- Learn how to [Think in assets](https://docs.dagster.io/guides/build/assets/)

- [What you'll learn](https://docs.dagster.io/getting-started/quickstart#what-youll-learn)
- [Prerequisites](https://docs.dagster.io/getting-started/quickstart#prerequisites)
- [Step 1: Set up the Dagster environment](https://docs.dagster.io/getting-started/quickstart#step-1-set-up-the-dagster-environment)
- [Step 2: Create the Dagster project structure](https://docs.dagster.io/getting-started/quickstart#step-2-create-the-dagster-project-structure)
- [Step 3: Define the assets](https://docs.dagster.io/getting-started/quickstart#step-3-define-the-assets)
- [Step 4: Run the pipeline](https://docs.dagster.io/getting-started/quickstart#step-4-run-the-pipeline)
- [Step 5: Verify the results](https://docs.dagster.io/getting-started/quickstart#step-5-verify-the-results)
- [Next steps](https://docs.dagster.io/getting-started/quickstart#next-steps)
