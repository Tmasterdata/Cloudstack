#!/usr/bin/env python
# coding: utf-8

# ## raw_to_curated
# 
# New notebook

# In[1]:


# These values can be set for development, but are overwritten by the orchestration pipeline on scheduled runs
TOPIC = None
WORKSPACE_NAME = None # Use None for all workspaces, otherwise use Workspace Name (Supplier, Item, etc.)
DEBUG = False
TRIGGER_FREQUENCY = None
SOURCE_TYPE = None # Pass in a Source Type to filter the metadata to
SCHEMA_LIST = None # Pass in Schema List as needed ['MC','DS','VSAM']   <-- Copy Paste List from here for debugging
TABLE_LIST = None  # Pass in a Table List as needed ['tableA', 'tableB']
PIPELINE_ID = 718   #None # Pass in a row via PipelineId


# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run Logging


# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run curated_etl_functions


# In[4]:


from sempy import fabric
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnull, regexp_replace, trim, when, lower, date_format, expr
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime, timedelta
import pytz
import time
#the following spark configurations are needed to handle dates that are less than 1900-01-01
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
# spark.conf.set("spark.ms.autotune.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "800")


# In[5]:


LOGGING_FLAG=False

#Mapping DFD = Dev, DFT = Test, DFP = Prod

# Get Current Workspace ID
CURRENT_WORKSPACE_ID=mssparkutils.runtime.context.get('currentWorkspaceId')
print(CURRENT_WORKSPACE_ID)
# Get Current Workspace Name
CURRENT_WORKSPACE_NAME = fabric.resolve_workspace_name(CURRENT_WORKSPACE_ID)
print(CURRENT_WORKSPACE_NAME)

#Parse Environment Name + workspace data foundation name
workspace_parse_list=CURRENT_WORKSPACE_NAME.split('-')
ENVIRONMENT_NAME=workspace_parse_list[0].strip()
DATA_FOUNDATION_NAME = workspace_parse_list[1].strip()
METADATA_LAKEHOUSE_ID=mssparkutils.lakehouse.get("Metadata",CURRENT_WORKSPACE_ID).id
print(METADATA_LAKEHOUSE_ID)
LOGGING_LAKEHOUSE_ID=mssparkutils.lakehouse.get("Logging",CURRENT_WORKSPACE_ID).id


RAW_LOG_DELTA_PATH = f"abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LOGGING_LAKEHOUSE_ID}/Tables/raw_snapshot_log"
CURATED_LOG_DELTA_PATH = f"abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LOGGING_LAKEHOUSE_ID}/Tables/curated_snapshot_log"


CURRENT_TS= datetime.now()
TIMEZONE = pytz.timezone('America/Chicago')
CURRENT_TS_TZ = CURRENT_TS.astimezone(TIMEZONE)
ROUNDED_TS = CURRENT_TS_TZ.replace(second=0, microsecond=0, minute=0) + timedelta(hours=CURRENT_TS.minute//30)
ROUNDED_TS = ROUNDED_TS.replace(tzinfo=None)


# # Create Curated Snapshot Log Table

# In[6]:


if DeltaTable.isDeltaTable(spark,CURATED_LOG_DELTA_PATH)==False:
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
    curated_log_df = spark.createDataFrame([], schema)
    curated_log_df.write.format('delta').save(CURATED_LOG_DELTA_PATH)

raw_log_df=spark.read.format('delta').load(RAW_LOG_DELTA_PATH)
raw_log_df.createOrReplaceTempView('raw_snapshot_log')
curated_log_df=spark.read.format('delta').load(CURATED_LOG_DELTA_PATH)
curated_log_df.createOrReplaceTempView('curated_snapshot_log')


# # Create Vsam Mapping Table

# In[7]:


metadata_lakehouse_path = f'abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{METADATA_LAKEHOUSE_ID}'
vsam_mapping_delta_path = f'{metadata_lakehouse_path}/Tables/vsam_mapping'

if DeltaTable.isDeltaTable(spark,vsam_mapping_delta_path)==False:
    # Read the parquet file into a DataFrame
    vsam_mapping_schema=StructType([StructField('vsam_file_name', StringType(), True),
    StructField('vsam_field_name', StringType(), True),
    StructField('col_order', IntegerType(), True),
    StructField('cobol_data_type', StringType(), True),
    StructField('spark_data_type', StringType(), True)])

    vsam_mapping_df = spark.read.option("header", "true").csv(f'{metadata_lakehouse_path}/Files/mapping/vsam_mapping.csv',schema=vsam_mapping_schema)
    # Write the DataFrame to the Delta table
    vsam_mapping_df.write.format('delta').mode('overwrite').save(vsam_mapping_delta_path)

vsam_mapping_df =spark.read.format('delta').load(vsam_mapping_delta_path)
vsam_mapping_df.createOrReplaceTempView('vsam_mapping')


# In[8]:


metadata_df = spark.read.format('delta').load(f'abfss://{CURRENT_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{METADATA_LAKEHOUSE_ID}/Tables/metadata_pipeline')
if PIPELINE_ID is None:
    metadata_df = metadata_df.select(regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"LoadType","DatabaseName","SourceSchema","SourceTable", "TargetSchema", "TargetTable","SourceFormat","TargetDestinationWorkspace","TargetDestinationLakehouse", "TargetWorkspaceName", "LoadByDateColumnName").filter((isnull(col("SourceTable"))==False) & (col("SourceLayer") == "OnPrem") & (isnull(col("PrimaryKey"))==False) & (col("Topic")==TOPIC) & (col("IsEnabled")==True)& (col("MergeFlag")==0))
else:
    metadata_df = metadata_df.select(regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"LoadType","DatabaseName","SourceSchema","SourceTable", "TargetSchema", "TargetTable","SourceFormat","TargetDestinationWorkspace","TargetDestinationLakehouse", "TargetWorkspaceName", "LoadByDateColumnName").filter((isnull(col("SourceTable"))==False) & (col("SourceLayer") == "OnPrem") & (isnull(col("PrimaryKey"))==False) & (col("PipelineId") == PIPELINE_ID) & (col("IsEnabled")==True)& (col("MergeFlag")==0))
# metadata_df = metadata_df.select(regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"LoadType","DatabaseName","SourceSchema","SourceTable", "TargetSchema", "TargetTable","SourceFormat","TargetDestinationWorkspace","TargetDestinationLakehouse", "TargetWorkspaceName", "LoadByDateColumnName").filter((isnull(col("SourceTable"))==False) & (col("SourceLayer") == "OnPrem") & (isnull(col("PrimaryKey"))==False) & (col("Topic")==TOPIC) & (col("IsEnabled")==True)& (col("DimensionLoadType")==0))
metadata_df = metadata_df.withColumn("IsEDW",when((col("TargetSchema").contains("edw") & lower(col("LoadByDateColumnName")).contains("start_dt")),lit(True)).otherwise(lit(False)))
metadata_df = metadata_df.orderBy("TargetWorkspaceName","TargetSchema","TargetTable")

# metadata_df = metadata_df.filter(col("DatabaseName") == 'GENESIS')

if PIPELINE_ID is None:
    if WORKSPACE_NAME is not None:
        metadata_df = metadata_df.orderBy("TargetTable").filter(col("TargetWorkspaceName")==WORKSPACE_NAME)

    if SOURCE_TYPE is not None:
        metadata_df = metadata_df.filter(col("SourceType")==SOURCE_TYPE)
        
    if TRIGGER_FREQUENCY is not None:
        metadata_df = metadata_df.filter(col("TriggerFrequency") == TRIGGER_FREQUENCY)

    if SCHEMA_LIST is not None:
        metadata_df = metadata_df.filter(col("SourceSchema").isin(SCHEMA_LIST))

    if TABLE_LIST is not None:
        metadata_df = metadata_df.filter(col("SourceTable").isin(TABLE_LIST))
else:
    metadata_df = metadata_df.filter(col("PipelineId") == PIPELINE_ID)

#Testing
if DEBUG:
    print("DEBUG MODE")   
    # metadata_df = metadata_df.filter(col("IsEDW")==True)
    # metadata_df= metadata_df.where("SourceTable ='WD2D_DT_TBL'")

if(LOGGING_FLAG):
    curated_log_df=spark.read.format('delta').load(CURATED_LOG_DELTA_PATH).where(f"LAST_LOADED_TS = '{ROUNDED_TS}' and is_successful_flag=1")
    # display(curated_log_df)
    curated_log_df = curated_log_df.withColumnRenamed('schema','TargetSchema')
    curated_log_df = curated_log_df.withColumnRenamed('table','TargetTable')
    metadata_df=metadata_df.join(curated_log_df, how='leftanti', on=['TargetSchema', 'TargetTable'])

display(metadata_df)


# # Set up for Specific Table Transformations

# In[9]:


metadata_list=[row.asDict() for row in metadata_df.collect()]
dataframe_dict={}
PreviousWorkspaceName = 'Dummy'

for row in metadata_list:
    target_schema_name = row['TargetSchema']
    target_table_name = row['TargetTable']
    key_list = row['PrimaryKey']
    print(key_list)
    is_edw = row['IsEDW']
    source_format =row['SourceFormat']

    target_workspace_name= row['TargetWorkspaceName']
    if target_workspace_name == 'sales_invoice':
        target_workspace_name = 'Sales & Invoice'

    
    if target_workspace_name != PreviousWorkspaceName:
        target_workspace_id=fabric.resolve_workspace_id(f"{ENVIRONMENT_NAME} - {target_workspace_name}")
        raw_lakehouse_id=mssparkutils.lakehouse.get(f"{row['TargetWorkspaceName'].lower().replace(' ','_')}_raw",target_workspace_id).id
        curated_lakehouse_id=mssparkutils.lakehouse.get(f"{row['TargetWorkspaceName'].lower().replace(' ','_')}_curated",target_workspace_id).id
        PreviousWorkspaceName = target_workspace_name




    try:



        raw_lakehouse_path= f'abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{raw_lakehouse_id}'
        source_df = spark.read.format('delta').load(f"{raw_lakehouse_path}/Tables/{target_schema_name}_{target_table_name}_raw")


        if(source_format=='VSAM'):

            source_df=cast_vsam_table(source_df,f"{target_schema_name}_{target_table_name}")

        # Load Incrementally or not
        if(row['LoadType'].strip()=='Incremental'):
            incremental_load_flag=True
            table_last_loaded_ts=raw_log_df.where(f"table='{target_table_name}'").orderBy(col('last_loaded_ts').desc()).select(col("last_loaded_ts").cast('string')).first()['last_loaded_ts']
            # table_last_loaded_ts = '2024-10-09 17:00:00.000000'
            source_df=source_df.where(f"LAST_LOADED_TS='{table_last_loaded_ts}'")
        else:
            incremental_load_flag=False

        
        dataframe_dict[(target_schema_name, target_table_name)] = {
            'key_list':key_list,
            'source_df':source_df,
            'is_edw':is_edw,
            'target_workspace_id':target_workspace_id,
            'raw_lakehouse_id':raw_lakehouse_id,
            'curated_lakehouse_id':curated_lakehouse_id,
            'incremental_load_flag':incremental_load_flag
        }


    except Exception as e:
        print(f"('{target_schema_name}','{target_table_name}') Key Failed to load")
        print(e)
        continue
if DEBUG:
    print(dataframe_dict.keys())
dataframe_dict_keys = dataframe_dict.keys()


# ## ('edw_mc', 'wf1i_invc_sls_dtl') Specific Transformation

# In[10]:


if ('edw_mc', 'wf1i_invc_sls_dtl') in dataframe_dict.keys():
    date_workspace_id = fabric.resolve_workspace_id(f"{ENVIRONMENT_NAME} - Date")
    date_lakehouse_id = mssparkutils.lakehouse.get(f"date_curated",date_workspace_id).id
    date_lakehouse_path = f'abfss://{date_workspace_id}@onelake.dfs.fabric.microsoft.com/{date_lakehouse_id}'

    date_df = spark.read.format('delta').load(f"{date_lakehouse_path}/Tables/edw_mc_wd2d_dt_tbl_curated").where("IS_CURRENT=1").select(col("DT_SID").alias('NEW_DT_SID'),'OLD_DT_SID')

    dataframe_dict[('edw_mc', 'wf1i_invc_sls_dtl')]['source_df']=dataframe_dict[('edw_mc', 'wf1i_invc_sls_dtl')]['source_df'].alias("incoming").join(
        date_df.alias("date"),
        on=expr("incoming.dt_sid=date.old_dt_sid"),
        how='inner'
    ).withColumn("DT_SID",col("NEW_DT_SID")).drop("NEW_DT_SID",col("OLD_DT_SID"))

    # Checks partition for this table.
    curated_wf1i_path=f"abfss://{dataframe_dict[('edw_mc', 'wf1i_invc_sls_dtl')]['target_workspace_id']}@onelake.dfs.fabric.microsoft.com/{dataframe_dict[('edw_mc', 'wf1i_invc_sls_dtl')]['curated_lakehouse_id']}/Tables/edw_mc_wf1i_invc_sls_dtl_curated"
    if DeltaTable.isDeltaTable(spark,curated_wf1i_path):
        # Add partitioning if its missing.

        curated_delta_table=DeltaTable.forPath(spark,curated_wf1i_path)

        target_partition_columns=curated_delta_table.detail().select("partitionColumns").first()['partitionColumns']

        wf1i_partition_columns = ['DT_SID']

        if (wf1i_partition_columns!=target_partition_columns):
            print("Repartitioning wf1i. This may take a while.")
            curated_wf1i_df= spark.read.format('delta').load(curated_wf1i_path)
            curated_wf1i_df.write.partitionBy(target_partition_columns).format("delta").mode("overwrite").option("overwriteSchema",'true').save(curated_wf1i_path)


# In[11]:


if DEBUG:
    dataframe_dict


# # Execute Raw to Curated Table Specific Transformations

# ## Date Dim Specific Transformation (example)

# In[12]:


# Example Transformation
if ('edw_mc', 'wd2d_dt_tbl') in dataframe_dict.keys():
    dataframe_dict[('edw_mc', 'wd2d_dt_tbl')]['source_df']=dataframe_dict[('edw_mc', 'wd2d_dt_tbl')]['source_df'].withColumn("OLD_DT_SID", col("DT_SID")).withColumn("DT_SID", date_format(col("DT"), "yyyyMMdd").cast("int"))


# In[13]:


# Example Transformation
# if ('mbm_mbmdtaprd', 'arm1') in dataframe_dict.keys():
#     dataframe_dict[('mbm_mbmdtaprd', 'arm1')]['source_df']=dataframe_dict[('mbm_mbmdtaprd', 'arm1')]['source_df'].withColumn("OLD_DT_SID", col("DT_SID")).withColumn("DT_SID", date_format(col("DT"), "yyyyMMdd").cast("int"))


# In[14]:


# Example Transformation
# if ('mbm_mbmdtaprd', 'arm3') in dataframe_dict.keys():
#     dataframe_dict[('mbm_mbmdtaprd', 'arm3')]['source_df']=dataframe_dict[('mbm_mbmdtaprd', 'arm3')]['source_df'].withColumn("OLD_DT_SID", col("DT_SID")).withColumn("DT_SID", date_format(col("DT"), "yyyyMMdd").cast("int"))


# In[15]:


# Example Transformation
# if ('mbm_mbmdtaprd', 'tr80') in dataframe_dict.keys():
#     dataframe_dict[('mbm_mbmdtaprd', 'tr80')]['source_df']=dataframe_dict[('mbm_mbmdtaprd', 'tr80')]['source_df'].withColumn("OLD_DT_SID", col("DT_SID")).withColumn("DT_SID", date_format(col("DT"), "yyyyMMdd").cast("int"))


# In[16]:


# Execute merge_scd2 function in parallel
exceptions = []
results = []
with ThreadPoolExecutor() as executor:
    # Submit tasks to executor
    futures = {
    executor.submit(
        merge_scd2, 
        schema_name, 
        table_name, 
        data['key_list'], 
        ROUNDED_TS, 
        data['source_df'],
        data['target_workspace_id'],
        data['raw_lakehouse_id'],
        data['curated_lakehouse_id'],
        data['is_edw'],
        DEBUG,
        data['incremental_load_flag']
    ): (schema_name, table_name) 
    for (schema_name, table_name), data in dataframe_dict.items()
    }
    # Wait for completion of tasks and handle results
    for future in as_completed(futures):
        schema_name, table_name = futures[future]
        try:
            results.append(future.result())
            # Process the result as needed
        except Exception as e:
            # Handle exceptions if needed
            exceptions.append(e)
            continue

if len(exceptions) == 0:
    for result in results:
        print(result)
else:
    for exception in exceptions:
        print(exception)
    raise Exception('The above tables have failed to be moved to Curated')

