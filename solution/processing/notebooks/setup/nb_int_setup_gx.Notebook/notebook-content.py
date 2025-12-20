# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# Runtime Environment configuration: 

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "environment": {
# MAGIC         "id": {"variableName": "$(/**/vl-int-variables/ENVIRONMENT_ID)"},
# MAGIC         "name": {"variableName": "$(/**/vl-int-variables/ENVIRONMENT_NAME)"}
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PRJ006 🔶 INT Project (Sprint 6): Setup Great Expectations Context
#  
# > The code in this notebook is written as part of Sprint 6 of the Intermediate Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). 
# 
# In this notebook, we: 
# 1. create some functions (refactored from existing code we wrote in DQ010) - we made the older code idempotent. 
# 2. We configure the parameters and Expectations for each of the three Gold-layer tables. We run each one. 
# 
# #### Step 1: Load in the simple GX setup functions I
# This is just a slight refactoring of the code we developed in [DQ010](https://www.skool.com/fabricdojo/classroom/8166498c?md=929849d57ec44d988d59ef1c6287fd6f). We added in some idempotency to make it nicer to work with. 

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

# Initialize context (at the default location "/tmp/gx")
context = gx.get_context(mode="file", project_root_dir="/tmp/gx_project/")

def get_or_create_datasource(source_type, params):
    """Get existing datasource or create new one.

    Args:
        source_type: One of "csv", "spark_df", or "semantic_model".
        params: Dict containing "datasource_name" and optionally "base_directory".

    Returns:
        GX DataSource object.
    """
    datasource_name = params['datasource_name']

    try:
        # Try to get existing datasource
        return context.data_sources.get(datasource_name)
    except Exception:
        # Doesn't exist, create it
        if source_type == 'csv':
            return context.data_sources.add_pandas_filesystem(
                name=datasource_name,
                base_directory=params['base_directory']
            )
        elif source_type == 'spark_df':
            return context.data_sources.add_spark(name=datasource_name)
        elif source_type == 'semantic_model':
            return context.data_sources.add_pandas(name=datasource_name)


def get_or_create_asset(datasource, source_type, params):
    """Get existing asset or create new one.

    Args:
        datasource: GX DataSource object.
        source_type: One of "csv", "spark_df", or "semantic_model".
        params: Dict containing "dataasset_name".

    Returns:
        GX DataAsset object.
    """
    asset_name = params['dataasset_name']

    try:
        return datasource.get_asset(asset_name)
    except Exception:
        if source_type == 'csv':
            return datasource.add_csv_asset(name=asset_name)
        else:
            return datasource.add_dataframe_asset(name=asset_name)


def get_or_create_batch_definition(asset, source_type, params):
    """Get existing batch definition or create new one.

    Args:
        asset: GX DataAsset object.
        source_type: One of "csv", "spark_df", or "semantic_model".
        params: Dict containing "dataasset_name".

    Returns:
        GX BatchDefinition object.
    """
    batch_def_name = f"{params['dataasset_name']}_batch_definition"

    try:
        return asset.get_batch_definition(batch_def_name)
    except Exception:
        if source_type == 'csv':
            return asset.add_batch_definition_path(
                name=batch_def_name,
                path=f"{params['dataasset_name']}.csv"
            )
        else:
            return asset.add_batch_definition_whole_dataframe(batch_def_name)


def get_or_create_suite(params, expectations_list):
    """Get existing expectation suite or create new one.

    Args:
        params: Dict containing "dataasset_name".
        expectations_list: List of GX Expectation objects.

    Returns:
        GX ExpectationSuite object.
    """
    suite_name = f"{params['dataasset_name']}_expectations"

    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = gx.ExpectationSuite(name=suite_name)
        suite = context.suites.add(suite)

    # Add any new expectations (GX handles duplicates)
    for expectation in expectations_list:
        try:
            suite.add_expectation(expectation)
        except Exception:
            pass  # Expectation already exists

    return suite


def get_or_create_validation_definition(batch_definition, suite, params):
    """Get existing validation definition or create new one.

    Args:
        batch_definition: GX BatchDefinition object.
        suite: GX ExpectationSuite object.
        params: Dict containing "dataasset_name".

    Returns:
        GX ValidationDefinition object.
    """
    validation_name = f"{params['dataasset_name']}_validation"

    try:
        validation_def = context.validation_definitions.get(validation_name)
    except Exception:
        validation_def = gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name=validation_name
        )
        validation_def = context.validation_definitions.add(validation_def)

    return validation_def


def register_any_datasource(source_type=None, params=None, expectations_list=None) -> None:
    """Idempotent registration - safe to run multiple times.

    Args:
        source_type: One of "csv", "spark_df" or "semantic_model".
        params: Dict with keys "datasource_name", "dataasset_name", "base_directory".
        expectations_list: List of GX Expectations for that dataset.
    """
    if params is None:
        params = {}
    if expectations_list is None:
        expectations_list = []

    # Step 1: Get or create datasource
    datasource = get_or_create_datasource(source_type, params)
    
    # Step 2: Get or create asset
    asset = get_or_create_asset(datasource, source_type, params)

    # Step 3: Get or create batch definition
    batch_definition = get_or_create_batch_definition(asset, source_type, params)
    
    # Step 4: Get or create expectation suite
    suite = get_or_create_suite(params, expectations_list)
    
    # Step 5: Get or create validation definition
    val_def = get_or_create_validation_definition(batch_definition, suite, params)
    
    print(f"Registered: {params['datasource_name']} / {params['dataasset_name']}")
    print(f"Validation Definition: {val_def.name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Configure params and Expectations for each dataset

# MARKDOWN ********************

# **Registering the Marketing Channels Gold Table:**

# CELL ********************

channels_params = {
    "datasource_name": "lh_int_gold", 
    "dataasset_name":"marketing_channels"
}

channels_expectations_list = [  
    gxe.ExpectColumnValuesToBeInSet(column="channel_surrogate_id", value_set=[1]),
    gxe.ExpectColumnValuesToBeIncreasing(column="channel_total_views")
]


register_any_datasource(source_type='spark_df', params=channels_params, expectations_list=channels_expectations_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Registering the Marketing Assets Gold Table:**

# CELL ********************

assets_params = {
    "datasource_name": "lh_int_gold", 
    "dataasset_name":"marketing_assets"
}

assets_expectations_list = [
    gxe.ExpectColumnValuesToBeUnique(column="asset_surrogate_id"), 
    gxe.ExpectColumnValuesToNotBeNull(column="asset_natural_id"),
    gxe.ExpectColumnValuesToNotBeNull(column="asset_published_date"), 
    gxe.ExpectCompoundColumnsToBeUnique(column_list=["asset_title", "asset_surrogate_id"])
]

register_any_datasource(source_type='spark_df', params=assets_params, expectations_list=assets_expectations_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Registering the Marketing Asset Stats Gold Table:**

# CELL ********************

asset_stats_params = {
    "datasource_name": "lh_int_gold", 
    "dataasset_name":"marketing_asset_stats"
}

asset_stats_expectations_list = [
    gxe.ExpectColumnValuesToBeNull(column="asset_total_impressions")
]


register_any_datasource(source_type='spark_df', params=asset_stats_params, expectations_list=asset_stats_expectations_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Move the GX Context into the Admin Lakehouse 
# Up until this 

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-int-variables")

admin_path = f"abfss://{variables.LH_WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{variables.ADMIN_LH_NAME}.Lakehouse"
local_path = "/tmp/gx_project/gx"
# remove any existing gx context (if exists)
destination = f"{admin_path}/Files/gx"
try:
    notebookutils.fs.rm(destination, recurse=True)
except Exception:
    pass  # Directory may not exist yet

# Copy GX context from local path to Admin Lakehouse
source = f"file://{local_path}"

notebookutils.fs.cp(source, destination, recurse=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
