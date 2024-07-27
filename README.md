# stdb
Python module for creating Synapse External Tables from Databricks

## Overview

`stdb` is a helper library designed to simplify the creation of Synapse SQL external tables from Databricks. Unlike Azure Synapse Studio, which defaults to `varchar(4000)` for string columns, this module uses spark to infers sane sizes for `varchar` columns, resulting in more efficient and accurate schema definitions.

## Features

- Automatically infers sensible sizes for `varchar` columns.
- Seamless integration with Databricks and Azure Synapse.
- Simplifies the process of creating external tables in Synapse from ADLS data using Databricks.

## Usage

```python
from stdb import Synapse

# Initialize the Synapse helper with necessary credentials and workspace name
# Parameters
# spark: Spark session object from Databricks.
# AZURE_TENANT_ID: Azure tenant ID for authentication.
# AZURE_CLIENT_ID: Azure client ID for authentication.
# AZURE_CLIENT_SECRET: Azure client secret for authentication.
# synapse_workspace: Name of the Synapse workspace.
synapse = Synapse(
    spark, 
    AZURE_TENANT_ID, 
    AZURE_CLIENT_ID, 
    AZURE_CLIENT_SECRET, 
    "synapse_workspace"
    )

# Create an external table in Synapse for ADLS tables
# Parameters
# adls_storage_account: Name of the ADLS storage account.
# container: Name of the container within the storage account.
# path: Path to the data within the container.
# synapse_database: Name of the target Synapse database.
synapse.create_external_table(
    'adls_storage_account', 
    'container', 
    'path/to/adls/database/', 
    'synapse_database'
    )
```

**Under heavy development, APIs may change.**
