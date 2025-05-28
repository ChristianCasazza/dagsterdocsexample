
On this page

Often in data engineering, you'll find yourself needing to create a large number of similar assets. For example:

- A set of database tables all have the same schema
- A set of files in a directory all have the same format

It's also possible you're serving stakeholders who aren't familiar with Python or Dagster. They may prefer interacting with assets using a domain-specific language (DSL) built on top of a configuration language such as YAML.

The asset factory pattern can solve both of these problems.

note

This article assumes familiarity with:

- [Assets](https://docs.dagster.io/guides/build/assets/defining-assets)
- [Resources](https://docs.dagster.io/guides/build/external-resources/)
- SQL, YAML, and Amazon Web Services (AWS) S3
- [Pydantic](https://docs.pydantic.dev/latest/) and [Jinja2](https://jinja.palletsprojects.com/en/3.1.x/)

Prerequisites

To run the code in this article, you'll need to create and activate a Python virtual environment and install the following dependencies:

```codeBlockLines_e6Vv
pip install dagster dagster-aws duckdb pyyaml pydantic

```

## Building an asset factory in Python [​](https://docs.dagster.io/guides/build/assets/creating-asset-factories\#building-an-asset-factory-in-python "Direct link to Building an asset factory in Python")

Let's imagine a team that often has to perform the same repetitive ETL task: download a CSV file from S3, run a basic SQL query on it, and then upload the result as a new file back to S3.

To automate this process, you might define an asset factory in Python like the following:

```codeBlockLines_e6Vv
import tempfile

import dagster_aws.s3 as s3
import duckdb

import dagster as dg

def build_etl_job(
    s3_resource: s3.S3Resource,
    bucket: str,
    source_object: str,
    target_object: str,
    sql: str,
) -> dg.Definitions:
    # asset keys cannot contain '.'
    asset_key = f"etl_{bucket}_{target_object}".replace(".", "_")

    @dg.asset(name=asset_key)
    def etl_asset(context):
        with tempfile.TemporaryDirectory() as root:
            source_path = f"{root}/{source_object}"
            target_path = f"{root}/{target_object}"

            # these steps could be split into separate assets, but
            # for brevity we will keep them together.
            # 1. extract
            context.resources.s3.download_file(bucket, source_object, source_path)

            # 2. transform
            db = duckdb.connect(":memory:")
            db.execute(
                f"CREATE TABLE source AS SELECT * FROM read_csv('{source_path}');"
            )
            db.query(sql).to_csv(target_path)

            # 3. load
            context.resources.s3.upload_file(bucket, target_object, target_path)

    return dg.Definitions(
        assets=[etl_asset],
        resources={"s3": s3_resource},
    )

s3_resource = s3.S3Resource(aws_access_key_id="...", aws_secret_access_key="...")

defs = dg.Definitions.merge(
    build_etl_job(
        s3_resource=s3_resource,
        bucket="my_bucket",
        source_object="raw_transactions.csv",
        target_object="cleaned_transactions.csv",
        sql="SELECT * FROM source WHERE amount IS NOT NULL;",
    ),
    build_etl_job(
        s3_resource=s3_resource,
        bucket="my_bucket",
        source_object="all_customers.csv",
        target_object="risky_customers.csv",
        sql="SELECT * FROM source WHERE risk_score > 0.8;",
    ),
)

```

The asset factory pattern is essentially a function that takes in some configuration and returns `dg.Definitions`.

## Configuring an asset factory with YAML [​](https://docs.dagster.io/guides/build/assets/creating-asset-factories\#configuring-an-asset-factory-with-yaml "Direct link to Configuring an asset factory with YAML")

Now, the team wants to be able to configure the asset factory using YAML instead of Python, with a file like this:

etl\_jobs.yaml

```codeBlockLines_e6Vv
aws:
  access_key_id: "YOUR_ACCESS_KEY_ID"
  secret_access_key: "YOUR_SECRET_ACCESS_KEY"

etl_jobs:
  - bucket: my_bucket
    source: raw_transactions.csv
    target: cleaned_transactions.csv
    sql: SELECT * FROM source WHERE amount IS NOT NULL

  - bucket: my_bucket
    source: all_customers.csv
    target: risky_customers.csv
    sql: SELECT * FROM source WHERE risk_score > 0.8

```

To implement this, parse the YAML file and use it to create the S3 resource and ETL jobs:

```codeBlockLines_e6Vv
import dagster_aws.s3 as s3
import yaml

import dagster as dg

def build_etl_job(
    s3_resource: s3.S3Resource,
    bucket: str,
    source_object: str,
    target_object: str,
    sql: str,
) -> dg.Definitions:
    # Code from previous example omitted
    return dg.Definitions()

def load_etl_jobs_from_yaml(yaml_path: str) -> dg.Definitions:
    config = yaml.safe_load(open(yaml_path))
    s3_resource = s3.S3Resource(
        aws_access_key_id=config["aws"]["access_key_id"],
        aws_secret_access_key=config["aws"]["secret_access_key"],
    )
    defs = []
    for job_config in config["etl_jobs"]:
        defs.append(
            build_etl_job(
                s3_resource=s3_resource,
                bucket=job_config["bucket"],
                source_object=job_config["source"],
                target_object=job_config["target"],
                sql=job_config["sql"],
            )
        )
    return dg.Definitions.merge(*defs)

defs = load_etl_jobs_from_yaml("etl_jobs.yaml")

```

## Improving usability with Pydantic and Jinja [​](https://docs.dagster.io/guides/build/assets/creating-asset-factories\#improving-usability-with-pydantic-and-jinja "Direct link to Improving usability with Pydantic and Jinja")

There are a few problems with the current approach:

1. **The YAML file isn't type-checked**, so it's easy to make mistakes that will cause cryptic `KeyError` s
2. **The YAML file contains secrets**. Instead, it should reference environment variables.

To solve these problems, you can use Pydantic to define a schema for the YAML file and Jinja to template the YAML file with environment variables.

Here's what the new YAML file might look like. Note how Jinja templating is used to reference environment variables:

etl\_jobs.yaml

```codeBlockLines_e6Vv
aws:
  access_key_id: "{{ env.AWS_ACCESS_KEY_ID }}"
  secret_access_key: "{{ env.AWS_SECRET_ACCESS_KEY }}"

etl_jobs:
  - bucket: my_bucket
    source: raw_transactions.csv
    target: cleaned_transactions.csv
    sql: SELECT * FROM source WHERE amount IS NOT NULL

  - bucket: my_bucket
    source: all_customers.csv
    target: risky_customers.csv
    sql: SELECT * FROM source WHERE risk_score > 0.8

```

And the Python implementation:

```codeBlockLines_e6Vv
import os
from typing import List

import dagster_aws.s3 as s3
import jinja2
import pydantic
import yaml

import dagster as dg

def build_etl_job(
    s3_resource: s3.S3Resource,
    bucket: str,
    source_object: str,
    target_object: str,
    sql: str,
) -> dg.Definitions:
    # Code from previous example omitted
    return dg.Definitions()

class AwsConfig(pydantic.BaseModel):
    access_key_id: str
    secret_access_key: str

    def to_resource(self) -> s3.S3Resource:
        return s3.S3Resource(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

class JobConfig(pydantic.BaseModel):
    bucket: str
    source: str
    target: str
    sql: str

    def to_etl_job(self, s3_resource: s3.S3Resource) -> dg.Definitions:
        return build_etl_job(
            s3_resource=s3_resource,
            bucket=self.bucket,
            source_object=self.source,
            target_object=self.target,
            sql=self.sql,
        )

class EtlJobsConfig(pydantic.BaseModel):
    aws: AwsConfig
    etl_jobs: list[JobConfig]

    def to_definitions(self) -> dg.Definitions:
        s3_resource = self.aws.to_resource()
        return dg.Definitions.merge(
            *[job.to_etl_job(s3_resource) for job in self.etl_jobs]
        )

def load_etl_jobs_from_yaml(yaml_path: str) -> dg.Definitions:
    yaml_template = jinja2.Environment().from_string(open(yaml_path).read())
    config = yaml.safe_load(yaml_template.render(env=os.environ))
    return EtlJobsConfig.model_validate(config).to_definitions()

defs = load_etl_jobs_from_yaml("etl_jobs_with_jinja.yaml")

```

- [Building an asset factory in Python](https://docs.dagster.io/guides/build/assets/creating-asset-factories#building-an-asset-factory-in-python)
- [Configuring an asset factory with YAML](https://docs.dagster.io/guides/build/assets/creating-asset-factories#configuring-an-asset-factory-with-yaml)
- [Improving usability with Pydantic and Jinja](https://docs.dagster.io/guides/build/assets/creating-asset-factories#improving-usability-with-pydantic-and-jinja)
