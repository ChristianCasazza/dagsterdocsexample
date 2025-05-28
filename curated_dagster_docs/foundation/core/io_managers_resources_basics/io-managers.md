
On this page

I/O managers in Dagster allow you to keep the code for data processing separate from the code for reading and writing data. This reduces repetitive code and makes it easier to change where your data is stored.

In many Dagster pipelines, assets can be broken down as the following steps:

1. Reading data a some data store into memory
2. Applying in-memory transform
3. Writing the transformed data to a data store

For assets that follow this pattern, an I/O manager can streamline the code that handles reading and writing data to and from a source.

note

This article assumes familiarity with: [assets](https://docs.dagster.io/guides/build/assets/) and [resources](https://docs.dagster.io/guides/build/external-resources/)

## Before you begin [​](https://docs.dagster.io/guides/build/io-managers\#before-you-begin "Direct link to Before you begin")

**I/O managers aren't required to use Dagster, nor are they the best option in all scenarios.** If you find yourself writing the same code at the start and end of each asset to load and store data, an I/O manager may be useful. For example:

- You have assets that are stored in the same location and follow a consistent set of rules to determine the storage path
- You have assets that are stored differently in local, staging, and production environments
- You have assets that load upstream dependencies into memory to do the computation

**I/O managers may not be the best fit if:**

- You want to run SQL queries that create or update a table in a database
- Your pipeline manages I/O on its own by using other libraries/tools that write to storage
- Your assets won't fit in memory, such as a database table with billions of rows

As a general rule, if your pipeline becomes more complicated in order to use I/O managers, it's likely that I/O managers aren't a good fit. In these cases you should use `deps` to [define dependencies](https://docs.dagster.io/guides/build/assets/passing-data-between-assets).

## Using I/O managers in assets [​](https://docs.dagster.io/guides/build/io-managers\#io-in-assets "Direct link to Using I/O managers in assets")

Consider the following example, which contains assets that construct a DuckDB connection object, read data from an upstream table, apply some in-memory transform, and write the result to a new table in DuckDB:

```codeBlockLines_e6Vv
import pandas as pd
from dagster_duckdb import DuckDBResource

import dagster as dg

raw_sales_data = dg.AssetSpec("raw_sales_data")

@dg.asset
def raw_sales_data(duckdb: DuckDBResource) -> None:
    # Read data from a CSV
    raw_df = pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Use the data from the CSV to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_sales_data AS SELECT * FROM raw_df"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO raw_sales_data SELECT * FROM raw_df")

# Asset dependent on `raw_sales_data` asset
@dg.asset(deps=[raw_sales_data])
def clean_sales_data(duckdb: DuckDBResource) -> None:
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Select data from table
        df = conn.execute("SELECT * FROM raw_sales_data").fetch_df()

        # Apply transform
        clean_df = df.fillna({"amount": 0.0})

        # Use transformed result to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS clean_sales_data AS SELECT * FROM clean_df"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO clean_sales_data SELECT * FROM clean_df")

# Asset dependent on `clean_sales_data` asset
@dg.asset(deps=[clean_sales_data])
def sales_summary(duckdb: DuckDBResource) -> None:
    # Construct DuckDB connection
    with duckdb.get_connection() as conn:
        # Select data from table
        df = conn.execute("SELECT * FROM clean_sales_data").fetch_df()

        # Apply transform
        summary = df.groupby(["owner"])["amount"].sum().reset_index()

        # Use transformed result to create or update a table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sales_summary AS SELECT * from summary"
        )
        if not conn.fetchall():
            conn.execute("INSERT INTO sales_summary SELECT * from summary")

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    resources={"duckdb": DuckDBResource(database="sales.duckdb", schema="public")},
)

```

Using an I/O manager would remove the code that reads and writes data from the assets themselves, instead delegating it to the I/O manager. The assets would be left only with the code that applies transformations or retrieves the initial CSV file.

```codeBlockLines_e6Vv
import pandas as pd
from dagster_duckdb_pandas import DuckDBPandasIOManager

import dagster as dg

@dg.asset
def raw_sales_data() -> pd.DataFrame:
    return pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")

@dg.asset
# Load the upstream `raw_sales_data` asset as input & specify the returned data type (`pd.DataFrame`)
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    # Storing data with an I/O manager requires returning the data
    return raw_sales_data.fillna({"amount": 0.0})

@dg.asset
def sales_summary(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    return clean_sales_data.groupby(["owner"])["amount"].sum().reset_index()

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    # Define the I/O manager and pass it to `Definitions`
    resources={
        "io_manager": DuckDBPandasIOManager(database="sales.duckdb", schema="public")
    },
)

```

To load upstream assets using an I/O manager, specify the asset as an input parameter to the asset function. In this example, the `DuckDBPandasIOManager` I/O manager will read the DuckDB table with the same name as the upstream asset ( `raw_sales_data`) and pass the data to `clean_sales_data` as a Pandas DataFrame.

To store data using an I/O manager, return the data in the asset function. The returned data must be a valid type. This example uses Pandas DataFrames, which the `DuckDBPandasIOManager` will write to a DuckDB table with the same name as the asset.

Refer to the individual I/O manager documentation for details on valid types and how they store data.

## Swapping data stores [​](https://docs.dagster.io/guides/build/io-managers\#swap-data-stores "Direct link to Swapping data stores")

With I/O managers, swapping data stores consists of changing the implementation of the I/O manager. The asset definitions, which only contain transformational logic, won't need to change.

In the following example, a Snowflake I/O manager replaced the DuckDB I/O manager.

```codeBlockLines_e6Vv
import pandas as pd
from dagster_snowflake_pandas import SnowflakePandasIOManager

import dagster as dg

@dg.asset
def raw_sales_data() -> pd.DataFrame:
    return pd.read_csv("https://docs.dagster.io/assets/raw_sales_data.csv")

@dg.asset
def clean_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    return raw_sales_data.fillna({"amount": 0.0})

@dg.asset
def sales_summary(clean_sales_data: pd.DataFrame) -> pd.DataFrame:
    return clean_sales_data.groupby(["owner"])["amount"].sum().reset_index()

defs = dg.Definitions(
    assets=[raw_sales_data, clean_sales_data, sales_summary],
    resources={
        # Swap in a Snowflake I/O manager
        "io_manager": SnowflakePandasIOManager(
            database=dg.EnvVar("SNOWFLAKE_DATABASE"),
            account=dg.EnvVar("SNOWFLAKE_ACCOUNT"),
            user=dg.EnvVar("SNOWFLAKE_USER"),
            password=dg.EnvVar("SNOWFLAKE_PASSWORD"),
        )
    },
)

```

## Built-in I/O managers [​](https://docs.dagster.io/guides/build/io-managers\#built-in "Direct link to Built-in I/O managers")

Dagster offers built-in library implementations for I/O managers for popular data stores and in-memory formats.

| Name | Description |
| --- | --- |
| [`FilesystemIOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.FilesystemIOManager) | Default I/O manager. Stores outputs as pickle files on the local file system. |
| [`InMemoryIOManager`](https://docs.dagster.io/api/dagster/io-managers#dagster.InMemoryIOManager) | Stores outputs in memory. Primarily useful for unit testing. |
| [`s3.S3PickleIOManager`](https://docs.dagster.io/api/libraries/dagster-aws#dagster_aws.s3.S3PickleIOManager) | Stores outputs as pickle files in Amazon Web Services S3. |
| [`adls2.ConfigurablePickledObjectADLS2IOManager`](https://docs.dagster.io/api/libraries/dagster-azure#dagster_azure.adls2.ConfigurablePickledObjectADLS2IOManager) | Stores outputs as pickle files in Azure ADLS2. |
| [`GCSPickleIOManager`](https://docs.dagster.io/api/libraries/dagster-gcp#dagster_gcp.GCSPickleIOManager) | Stores outputs as pickle files in Google Cloud Platform GCS. |
| [`BigQueryPandasIOManager`](https://docs.dagster.io/api/libraries/dagster-gcp-pandas#dagster_gcp_pandas.BigQueryPandasIOManager) | Stores Pandas DataFrame outputs in Google Cloud Platform BigQuery. |
| [`BigQueryPySparkIOManager`](https://docs.dagster.io/api/libraries/dagster-gcp-pyspark#dagster_gcp_pyspark.BigQueryPySparkIOManager) | Stores PySpark DataFrame outputs in Google Cloud Platform BigQuery. |
| [`SnowflakePandasIOManager`](https://docs.dagster.io/api/libraries/dagster-snowflake-pandas#dagster_snowflake_pandas.SnowflakePandasIOManager) | Stores Pandas DataFrame outputs in Snowflake. |
| [`SnowflakePySparkIOManager`](https://docs.dagster.io/api/libraries/dagster-snowflake-pyspark#dagster_snowflake_pyspark.SnowflakePySparkIOManager) | Stores PySpark DataFrame outputs in Snowflake. |
| [`DuckDBPandasIOManager`](https://docs.dagster.io/api/libraries/dagster-duckdb-pandas#dagster_duckdb_pandas.DuckDBPandasIOManager) | Stores Pandas DataFrame outputs in DuckDB. |
| [`DuckDBPySparkIOManager`](https://docs.dagster.io/api/libraries/dagster-duckdb-pyspark#dagster_duckdb_pyspark.DuckDBPySparkIOManager) | Stores PySpark DataFrame outputs in DuckDB. |
| [`DuckDBPolarsIOManager`](https://docs.dagster.io/api/libraries/dagster-duckdb-polars#dagster_duckdb_polars.DuckDBPolarsIOManager) | Stores Polars DataFrame outputs in DuckDB. |

## Next steps [​](https://docs.dagster.io/guides/build/io-managers\#next-steps "Direct link to Next steps")

- Learn to [connect databases](https://docs.dagster.io/guides/build/external-resources/connecting-to-databases) with resources
- Learn to [connect APIs](https://docs.dagster.io/guides/build/external-resources/connecting-to-apis) with resources

- [Before you begin](https://docs.dagster.io/guides/build/io-managers#before-you-begin)
- [Using I/O managers in assets](https://docs.dagster.io/guides/build/io-managers#io-in-assets)
- [Swapping data stores](https://docs.dagster.io/guides/build/io-managers#swap-data-stores)
- [Built-in I/O managers](https://docs.dagster.io/guides/build/io-managers#built-in)
- [Next steps](https://docs.dagster.io/guides/build/io-managers#next-steps)
