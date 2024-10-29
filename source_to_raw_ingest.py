#!/usr/bin/env python
# coding: utf-8

# ## source_to_raw_ingest
# 
# New notebook

# In[9]:


# These values can be set for development, but are overwritten by the orchestration pipeline on scheduled runs
TOPIC = None
WORKSPACE_NAME = None # Use None for all workspaces, otherwise use Workspace Name (Supplier, Item, etc.)
LOGGING_FLAG = True
TRIGGER_FREQUENCY = None # Pass in a trigger frequency to filter the metadata to
SOURCE_TYPE = None # Pass in a Source Type to filter the metadata to
SCHEMA_LIST = None # Pass in Schema List as needed ['MC','DS','VSAM'], ['VSAM']   <-- Copy Paste List from here for debugging
TABLE_LIST = None
# Pass in a Table List as needed ['tableA', 'tableB']
PIPELINE_ID = None # Pass in a row via PipelineId


# In[10]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run Logging


# In[11]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run raw_etl_functions


# # Import required libraries

# In[17]:


from sempy import fabric
from pyspark.sql.functions import current_timestamp,lit,col, lower, date_format, when, concat, isnull,sha2,concat_ws,trim,unhex,udf,regexp_replace
from pyspark.sql.types import *
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import struct
from pprint import pprint
from typing import List
from datetime import datetime, timedelta
import pytz

#the following spark configurations are needed to handle dates that are less than 1900-01-01
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.ms.autotune.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


# In[13]:


# Get Current Workspace ID
CURRENT_WORKSPACE_ID=mssparkutils.runtime.context.get('currentWorkspaceId')
# Get Current Workspace Name
CURRENT_WORKSPACE_NAME = fabric.resolve_workspace_name(CURRENT_WORKSPACE_ID)

#Parse Environment Name + workspace data foundation name
workspace_parse_list=CURRENT_WORKSPACE_NAME.split('-')
ENVIRONMENT_NAME=workspace_parse_list[0].strip()
DATA_FOUNDATION_NAME = workspace_parse_list[1].strip()
METADATA_LAKEHOUSE_ID=mssparkutils.lakehouse.get("Metadata",CURRENT_WORKSPACE_ID).id
LOGGING_LAKEHOUSE_ID=mssparkutils.lakehouse.get("Logging",CURRENT_WORKSPACE_ID).id


RAW_LOG_DELTA_PATH = f"abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LOGGING_LAKEHOUSE_ID}/Tables/raw_snapshot_log"
CURATED_LOG_DELTA_PATH = f"abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LOGGING_LAKEHOUSE_ID}/Tables/curated_snapshot_log"


CURRENT_TS= datetime.now()
TIMEZONE = pytz.timezone('America/Chicago')
CURRENT_TS_TZ = CURRENT_TS.astimezone(TIMEZONE)
ROUNDED_TS = CURRENT_TS_TZ.replace(second=0, microsecond=0, minute=0) + timedelta(hours=CURRENT_TS.minute//30)
ROUNDED_TS = ROUNDED_TS.replace(tzinfo=None)


# # Create Raw Snapshot Log Table

# In[14]:


raw_log_delta_path = f"abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LOGGING_LAKEHOUSE_ID}/Tables/raw_snapshot_log"
if DeltaTable.isDeltaTable(spark,raw_log_delta_path)==False:
    # Define the schema
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("last_loaded_ts", TimestampType(), True),
        StructField("is_successful_flag", BooleanType(), True),
        StructField("expired_records", IntegerType(), True),
        StructField("new_records", IntegerType(), True),
        StructField("updated_records", IntegerType(), True),
        StructField("unchanged_records", IntegerType(), True),
        StructField("error", StringType(), True),
        StructField("target_destination_workspace", StringType(), True),
        StructField("target_destination_lakehouse", StringType(), True)

    ])

    # Create an empty DataFrame with the defined schema
    raw_log_df = spark.createDataFrame([], schema)
    raw_log_df.write.format('delta').save(raw_log_delta_path)

raw_log_df=spark.read.format('delta').load(raw_log_delta_path)
raw_log_df.createOrReplaceTempView('raw_snapshot_log')


# # Initialize all the needed metadata

# In[15]:


metadata_df = spark.read.format('delta').load(f'abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{METADATA_LAKEHOUSE_ID}/Tables/metadata_pipeline')

if PIPELINE_ID is None:
    metadata_df = metadata_df.select("Topic",regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"SourceSchema","SourceTable", "TargetSchema", "TargetTable","SourceFormat","TargetWorkspaceName","TargetDestinationWorkspace","TargetDestinationLakehouse","MergeFilePath").filter((isnull(col("SourceTable"))==False) & (col("SourceLayer") == "OnPrem") & (isnull(col("PrimaryKey"))==False) & (col("IsEnabled")==True) & (col("Topic")==TOPIC) & (col("MergeFlag")==0))

    if WORKSPACE_NAME is not None:
        metadata_df = metadata_df.filter(col("TargetWorkspaceName")==WORKSPACE_NAME)
        
    if SOURCE_TYPE is not None:
        metadata_df = metadata_df.filter(col("SourceType") == SOURCE_TYPE)

    if TRIGGER_FREQUENCY is not None:
        metadata_df = metadata_df.filter(col("TriggerFrequency") == TRIGGER_FREQUENCY)

    if TABLE_LIST is not None:
        metadata_df = metadata_df.filter(col("SourceTable").isin(TABLE_LIST))

    if SCHEMA_LIST is not None:
        metadata_df = metadata_df.filter(col("SourceSchema").isin(SCHEMA_LIST))
else:
    metadata_df = metadata_df.select("Topic",regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"SourceSchema","SourceTable", "TargetSchema", "TargetTable","SourceFormat","TargetWorkspaceName","TargetDestinationWorkspace","TargetDestinationLakehouse","MergeFilePath").filter((isnull(col("SourceTable"))==False) & (col("SourceLayer") == "OnPrem") & (isnull(col("PrimaryKey"))==False) & (col("IsEnabled")==True) & (col("PipelineId") == PIPELINE_ID) & (col("MergeFlag")==0))


if(LOGGING_FLAG):
    raw_log_df=spark.read.format('delta').load(raw_log_delta_path).where(f"LAST_LOADED_TS = '{ROUNDED_TS}' and is_successful_flag=1")
    raw_log_df = raw_log_df.withColumnRenamed('schema','TargetSchema')
    raw_log_df = raw_log_df.withColumnRenamed('table','TargetTable')
    metadata_df=metadata_df.join(raw_log_df, how='leftanti', on=['TargetSchema', 'TargetTable'])


display(metadata_df)


# # Run Merge Operations

# In[18]:


# ThreadPoolExecutor Block
metadata_list=metadata_df.collect()
exceptions = []
with ThreadPoolExecutor() as executor:
    futures_dict = {f"{row['TargetSchema']}_{row['TargetTable']}":executor.submit(merge_to_raw, row,ROUNDED_TS, LOGGING_FLAG) for row in metadata_list}

for key in futures_dict.keys():
    try:
        futures_dict[key]=futures_dict[key].result()
    except Exception as e:
            # Handle exceptions if needed
            exceptions.append(e)
            continue
print()
if len(exceptions) == 0:
    pprint(futures_dict, compact=True)
else:
    for exception in exceptions:
        print(exception)
    raise Exception('The above tables have failed to be moved to Raw')

