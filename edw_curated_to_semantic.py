#!/usr/bin/env python
# coding: utf-8

# ## edw_curated_to_semantic
# 
# New notebook

# In[1]:


STAGING_TABLE = None


# In[1]:


from sempy import fabric
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime, timedelta
import pytz
from typing import List
from delta.tables import DeltaTable
#the following spark configurations are needed to handle dates that are less than 1900-01-01
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")


# In[2]:


#Get Workspace Environment
CURRENT_WORKSPACE_ID=mssparkutils.runtime.context.get('currentWorkspaceId')
CURRENT_WORKSPACE_NAME = fabric.resolve_workspace_name(CURRENT_WORKSPACE_ID)
workspace_parse_list=CURRENT_WORKSPACE_NAME.split('-')
ENVIRONMENT_NAME=workspace_parse_list[0].strip()
DATA_FOUNDATION_NAME = workspace_parse_list[1].strip()
CURRENT_DEFAULT_LAKEHOUSE_ID = fabric.get_lakehouse_id()

DEBUG = False


# #### Pull Metadata for EDW Layer to convert pre-existing EDW Tables to use the correct names

# In[4]:


metadata_df = spark.read.load('abfss://af50e063-fb6c-4a72-ac47-97306bc62af9@onelake.dfs.fabric.microsoft.com/7d3bc150-7ea5-4154-acc8-c27058bea456/Tables/edw_control_table')
metadata_df = metadata_df.select(regexp_replace(trim("PrimaryKey"), ' ', '').alias("PrimaryKey"),"SourceTable", "TargetTable").filter((col("SourceLayer") == "Curated") & (isnull(col("PrimaryKey"))==False) & (col("IsEnabled")==True))
#Testing
if DEBUG:   
    metadata_df = metadata_df.filter(col("TargetTable").isin(['profile_dim', 'converted_item_dim', 'tax_product_type_dim']))
if STAGING_TABLE is not None:
    metadata_df = metadata_df.filter(col("TargetTable") == STAGING_TABLE)
display(metadata_df)


# In[5]:


default_lakehouse_path = 'abfss://af50e063-fb6c-4a72-ac47-97306bc62af9@onelake.dfs.fabric.microsoft.com/7d3bc150-7ea5-4154-acc8-c27058bea456/Tables/'


# In[6]:


def build_join_condition(keys: List[str]) -> str:
    join_condition = f"ON tgt.{keys[0][0]} = src.{keys[0][0]}"
    if len(keys[0]) > 1:
        join_condition += " AND " + " AND ".join([f"tgt.{key} = src.{key}" for key in keys[0][1:]])
    return join_condition


# In[7]:


# Loop through metadata and write landing files to Delta format
def merge_scd2(source_table_name:str, 
target_table_name:str, 
key_list:List, 
Debug: bool):

    keys = [key_list.split(",")]
    join_condition=build_join_condition(keys)
    source_path = f"{default_lakehouse_path}/{source_table_name}"
    target_path = f"{default_lakehouse_path}/{target_table_name}"
    try:
        if DeltaTable.isDeltaTable(spark, source_path):
            sourcedf = spark.read.load(source_path) #TODO Add filter for last 15 days after renaming START & END DATE
            sourceRowCount = sourcedf.count()        
            sourcedf.createOrReplaceTempView(f"source_{target_table_name}")              
            if DeltaTable.isDeltaTable(spark,target_path):
                target_df = spark.read.load(target_path)
                # Update Target Schema In Place if the source schema changed - e.g. added new columns
                if set(sourcedf.columns) != set(target_df.columns):
                    # Alter table to add missing columns
                    if DEBUG:
                        sourcedf_schema=sourcedf.schema
                        targetdf_schema=target_df.schema
                        print(f"Source Schema {source_table_name}:{sourcedf.printSchema()}")
                        print(f"Target Schema {target_table_name}:{target_df.printSchema()}")
                    # Get columns present in the DataFrame
                    existing_columns = set(target_df.columns)

                    # Columns in the schema that are missing in the DataFrame
                    missing_columns = [c for c in sourcedf_schema.fields if c.name not in existing_columns]
                    # print(f"{target_table_name} missing columns: {missing_columns}")
                    # Add missing columns to the DataFrame with default values or null
                    for co in missing_columns:
                        target_df = target_df.withColumn(co.name, lit(None).cast(co.dataType))                    
                    first_columns = [ele for ele in target_df.columns if ele not in audit_columns]
                    select_columns = first_columns+audit_columns 

                    target_df=target_df.select(*select_columns)
                    target_df.write.format('delta').mode('overwrite').option("overwriteSchema", "true").save(f"{target_table_name}")
                    print(f"--> Schema Altered: {target_table_name} ")           
                target_df.createOrReplaceTempView(f"target_{target_table_name}") 
                updates_inserts = f"MERGE INTO target_{target_table_name} tgt USING source_{target_table_name} src {join_condition} " \
                                f"WHEN MATCHED AND tgt.IS_CURRENT != src.IS_CURRENT " \
                                f"THEN UPDATE SET * " \
                                f"WHEN NOT MATCHED " \
                                f"THEN INSERT *; "

                result1 = spark.sql(updates_inserts)

                updatedRecords = result1.collect()[0][1] if not None else 0
                newRecords = result1.collect()[0][3] if not None else 0
                deletedRecords = 0
                expiredRecords = 0
                unchangedRecords = sourceRowCount - updatedRecords

                if expiredRecords > 0 or newRecords > 0 or deletedRecords > 0 or updatedRecords > 0:
                    print(f"{target_table_name} --> Expired Rows: {expiredRecords} || New Rows: {newRecords} || Updated Rows: {updatedRecords} || Deleted Rows: {deletedRecords} || Unchanged Rows: {unchangedRecords}")
                else:
                    print(f"{target_table_name} --> No Changes: {unchangedRecords}")
            else:
                source_delta_table = DeltaTable.forPath(spark, source_path)
                # Retrieve partition columns using DeltaTable API
                source_details_df = source_delta_table.detail()
                source_partition_columns = source_details_df.select("partitionColumns").collect()[0]['partitionColumns']
                sourcedf.write.mode("overwrite").option("overwriteSchema", "true").partitionBy(source_partition_columns).save(target_path)
        else:
            print(f"source table {source_table_name} missing")
    except Exception as e:
        if e is not None:
            print(f"{target_table_name} Exception: {e}")


# In[8]:


metadata_list=[row.asDict() for row in metadata_df.collect()]
dataframe_dict={}
for row in metadata_list:
    source_table = row['SourceTable']
    target_table = row['TargetTable']
    key_list = row['PrimaryKey']
    try:
        dataframe_dict[(source_table, target_table)] = {
            'key_list':key_list,
        }
    except Exception as e:
        print(f"('{source_table}','{target_table}') Key Failed to load")
        print(e)
        continue
print(dataframe_dict.keys())
dataframe_dict_keys = dataframe_dict.keys()


# In[10]:


with ThreadPoolExecutor() as executor:
    # Submit tasks to executor
    futures = {
    executor.submit(
        merge_scd2, 
        source_table, 
        target_table, 
        data['key_list'], 
        DEBUG
    ): (source_table, target_table) 
    for (source_table, target_table), data in dataframe_dict.items()
    }
    # Wait for completion of tasks and handle results
    for future in as_completed(futures):
        source_table, target_table = futures[future]
        try:
            result = future.result()
            print(result)
            # Process the result as needed
        except Exception as e:
            # Handle exceptions if needed
            print(e)
            continue


# In[ ]:


# if STAGING_TABLE == 'profile_dim':
#     delta_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wd1f_prof_curated")
#     delta_df.write.format('delta').save(f"{default_lakehouse_path}/profile_dim")


# In[ ]:


# akvuppu/DCSIP-352/3-May
# if STAGING_TABLE == 'invoice_sales_detail_fct':
#     invoice_sales_detail_fct_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wf1i_invc_sls_dtl_curated")
#     invoice_sales_detail_fct_df.write.format('delta').save(f"{default_lakehouse_path}/invoice_sales_detail_fct")


# In[ ]:


# if STAGING_TABLE == 'out_stock_dim':
#     delta_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wd1o_out_stk_tbl_curated")
#     delta_df.write.format('delta').save(f"{default_lakehouse_path}/out_stock_dim")


# In[ ]:


# if STAGING_TABLE == 'credit_reason_dim':
#     delta_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wd2c_crdt_rsn_tbl_curated")
#     delta_df.write.format('delta').save(f"{default_lakehouse_path}/credit_reason_dim")


# In[ ]:


# if STAGING_TABLE == 'item_sales_type_dim':
#     delta_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wd3s_item_sls_typ_tbl_curated")
#     delta_df.write.format('delta').save(f"{default_lakehouse_path}/item_sales_type_dim")


# In[ ]:


# if STAGING_TABLE == 'item_history_weekly_snpsht_fct':
#     deltafhis_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfhis_curated").withColumnRenamed("DCSFHIS_SID","HISTORY_ITEM_SID")
#     deltafhis_df.write.mode("overwrite").option("overwriteSchema", "true").format('delta').save(f"{default_lakehouse_path}/item_history_weekly_snpsht_fct")


# In[ ]:


if STAGING_TABLE == 'division_item_department_dim':
    delta_df = spark.read.load(f"{default_lakehouse_path}/edw_mc_wd4d_div_item_dept_curated")
    delta_df.write.format('delta').mode("overwrite").save(f"{default_lakehouse_path}/division_item_department_dim")


# In[ ]:


if STAGING_TABLE == 'item_history_weekly_snpsht_fct':
    deltafhis_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfhis_curated").withColumnRenamed("DCSFHIS_SID","HISTORY_ITEM_SID")
    df1 = deltafhis_df.withColumn("date_new",date_format(to_date(col("HISTORY_DATE_YYDDD"), format="yyyyDDD"),"yyyyMMdd")).drop("HISTORY_DATE_YYDDD")\
    .withColumnRenamed("date_new","HISTORY_DATE_YYDDD")
    df1.write.mode("overwrite").option("overwriteSchema", "true").format('delta').save(f"{default_lakehouse_path}/item_history_weekly_snpsht_fct")


# In[ ]:


if STAGING_TABLE == 'purchase_order_outstanding_fct':
    from pyspark.sql.functions import col, lit, from_unixtime, unix_timestamp, to_date, date_add, floor,to_str, concat, when, date_format, desc
    from pyspark.sql.column import cast
    from pyspark.sql.types import StringType, IntegerType
    # .select(dcsfpod_df.DIV_ID,dcsfpod_df.ITEM_NUMBER,dcsfpoh_df.PO_STATUS_CODE)
    #convert date to yyyyMMdd from yyyyDDD and default to 2999-01-01 or 1900-01-01 if outside date range for all dates
    stat_list = ['A','E','F','G','H','I','K','L','M','N','O','P']
    dcsfpod_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfpod_curated")
    dcsfpoh_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfpoh_curated")
    delta_df = dcsfpod_df.join(dcsfpoh_df,((dcsfpoh_df.PO_NBR == dcsfpod_df.PO_NBR) & (dcsfpoh_df.PO_LEVEL == dcsfpod_df.PO_LEVEL)),"inner")\
    .select(dcsfpod_df.DIV_ID,dcsfpod_df.VENDOR_NUMBER,dcsfpod_df.ITEM_NUMBER,\
    date_format(when(date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000)).otherwise(to_date(lit("2999-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('KEY_DATE_YYDDD'),\
    dcsfpod_df.PO_NBR,dcsfpod_df.PO_LEVEL,dcsfpod_df.PO_LINE_NBR,dcsfpod_df.PO_LINE_NBR_SUFFIX,\
    dcsfpod_df.PO_KEY_STATUS,dcsfpod_df.SHIPTO_DIVISION,dcsfpod_df.SHIPTO_WHSE,\
    dcsfpod_df.BUYER_IDX,dcsfpod_df.DEPT_CODE,dcsfpod_df.TOTAL_BOH,dcsfpod_df.TOTAL_ON_ORDER,\
    dcsfpod_df.TOTAL_CASES_PRCHSD,dcsfpod_df.TOTAL_CASES_RCVD,dcsfpod_df.TOTAL_WEIGHT_RCVD,\
    date_format(when(date_add(to_date(concat(floor(dcsfpod_df.RCVD_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.RCVD_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpod_df.RCVD_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.RCVD_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('RCVD_YYDDD'),\
    dcsfpod_df.RCVD_HHMM,dcsfpoh_df.PO_STATUS_CODE,\
    date_format(when(date_add(to_date(concat(floor(dcsfpoh_df.DELIVERY_DUE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.DELIVERY_DUE_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpoh_df.DELIVERY_DUE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.DELIVERY_DUE_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('DELIVERY_DUE_YYDDD'),\
    date_format(when(date_add(to_date(concat(floor(dcsfpoh_df.ORDERED_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.ORDERED_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpoh_df.ORDERED_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.ORDERED_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('ORDERED_YYDDD'),\
    dcsfpod_df.CASES_AVAILABLE,dcsfpod_df.WTD_MOVEMENT,dcsfpod_df.WTD_STOCKOUT,\
    dcsfpod_df.ACTUAL_ORDER_TYPE_1,dcsfpod_df.ACTUAL_ORDER_QTY_1,dcsfpod_df.INVENTORY_COST,dcsfpod_df.INVOICE_COST,\
    dcsfpoh_df.INVOICE_COST_CODE_1,dcsfpoh_df.AMOUNT_1,\
    dcsfpoh_df.AMOUNT_TYPE_CODE_1,dcsfpoh_df.INVENTORY_COST_CODE_1,\
    dcsfpod_df.BOH_AT_RCVNG,dcsfpod_df.EXCEPTION_CASES,dcsfpod_df.BACKORDER_RCVNG_CODE,dcsfpod_df.GALLONS_PER_CASE,\
    dcsfpod_df.NEXT_COST,dcsfpod_df.LAST_RECEIVED_COST,dcsfpod_df.DEAL_NUMBER,dcsfpod_df.DEAL_ON_ORDER_QTY,\
    dcsfpod_df.ORIG_PO_BRACKET,dcsfpod_df.ITEM_LIST_COST,dcsfpod_df.DISPERSED_FLAG,\
    dcsfpod_df.EFFECTIVE_START_DATE,dcsfpod_df.EFFECTIVE_END_DATE,dcsfpod_df.IS_CURRENT)
    delta_df = delta_df.orderBy(desc("KEY_DATE_YYDDD")).dropDuplicates(["PO_NBR","PO_LINE_NBR","DIV_ID"])
    delta_df = delta_df.filter(col("PO_STATUS_CODE").isin(stat_list))
    delta_df.write.mode("overwrite").option("overwriteSchema", "true").format('delta').save(f"{default_lakehouse_path}/purchase_order_outstanding_fct")
#display(delta_df.show(10))


# In[ ]:


# akvuppu/DCSIP-450/6-May
if STAGING_TABLE == 'item_inventory_daily_snpsht_fct':
    dcsfitm_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfitm_curated")
    dcsfadm_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfadm_curated")

    item_inventory_daily_snpsht_fct_df = dcsfitm_df.join( \
        dcsfadm_df, \
        ((dcsfitm_df.DIV_ID == dcsfadm_df.DIV_ID) & \
        (dcsfitm_df.RECORD_KEY == dcsfadm_df.ITEM_NUMBER) & \
        (dcsfitm_df.IS_CURRENT == True) & \
        (dcsfadm_df.IS_CURRENT == True)), \
        how="left").select(dcsfitm_df.DIV_ID,dcsfitm_df.RECORD_KEY,dcsfitm_df.VENDOR_NBR,dcsfitm_df.AVG_WKLY_MVMNT,dcsfitm_df.TOTAL_BOH,\
        dcsfitm_df.D_PRODUCT_ON_HAND,dcsfitm_df.WTD_STOCKOUT,dcsfitm_df.BUYING_SYSTEM_FL,dcsfitm_df.TEMP_OUT_OF_STOCK_FL,dcsfitm_df.NEXT_DUE_CASES,dcsfitm_df.ITEMNOFULL,\
        dcsfitm_df.SHIP_CASE_CUBE,dcsfitm_df.PICK_SLOT_NUMBER_MSC,dcsfitm_df.PICK_SLOT_WHSE,dcsfitm_df.REPL_ITM,dcsfitm_df.VENDOR_CASE_CUBE,dcsfitm_df.BUYING_MULTIPLE,\
        dcsfitm_df.MULTIPLE_W_V_FLAG,dcsfitm_df.MULTIPLE_P_T_FLAG,dcsfitm_df.MULTIPLE_MULTIPLIER,dcsfitm_df.TI,dcsfitm_df.HI,dcsfitm_df.REPACK_FACTOR,dcsfitm_df.VENDOR_TI,\
        dcsfitm_df.VENDOR_HI,dcsfitm_df.TOTAL_DUE_IN_CASES,dcsfitm_df.ITEMNOUNIT,dcsfitm_df.SHIP_CASE_WEIGHT,dcsfitm_df.TERMINATION_DATE,dcsfitm_df.VENDOR_CASE_WEIGHT,\
        dcsfitm_df.YTD_DISTRIBUTION,dcsfitm_df.YTD_MOVEMENT,dcsfitm_df.YTD_OUT_OF_STOCKS,dcsfitm_df.WTD_DISTRIBUTION,dcsfitm_df.WTD_MOVEMENT,dcsfitm_df.WEEK_DISTRIBUTION_1,\
        dcsfitm_df.WEEK_DISTRIBUTION_2,dcsfitm_df.WEEK_DISTRIBUTION_3,dcsfitm_df.WEEK_DISTRIBUTION_4,dcsfitm_df.WEEK_DISTRIBUTION_5,dcsfitm_df.WEEK_DISTRIBUTION_6,dcsfitm_df.WEEK_DISTRIBUTION_7,\
        dcsfitm_df.WEEK_DISTRIBUTION_8,dcsfitm_df.WEEK_DISTRIBUTION_9,dcsfitm_df.WEEK_DISTRIBUTION_10,dcsfitm_df.WEEK_DISTRIBUTION_11,dcsfitm_df.WEEK_DISTRIBUTION_12,dcsfitm_df.WEEK_DISTRIBUTION_13,\
        dcsfitm_df.WEEK_MOVEMENT_1,dcsfitm_df.WEEK_MOVEMENT_2,dcsfitm_df.WEEK_MOVEMENT_3,dcsfitm_df.WEEK_MOVEMENT_4,dcsfitm_df.WEEK_MOVEMENT_5,dcsfitm_df.WEEK_MOVEMENT_6,dcsfitm_df.WEEK_MOVEMENT_7,\
        dcsfitm_df.WEEK_MOVEMENT_8,dcsfitm_df.WEEK_MOVEMENT_9,dcsfitm_df.WEEK_MOVEMENT_10,dcsfitm_df.WEEK_MOVEMENT_11,dcsfitm_df.WEEK_MOVEMENT_12,dcsfitm_df.WEEK_MOVEMENT_13,dcsfitm_df.WEEK_OUT_OF_STOCKS_1,\
        dcsfitm_df.WEEK_OUT_OF_STOCKS_2,dcsfitm_df.WEEK_OUT_OF_STOCKS_3,dcsfitm_df.WEEK_OUT_OF_STOCKS_4,dcsfitm_df.WEEK_OUT_OF_STOCKS_5,dcsfitm_df.WEEK_OUT_OF_STOCKS_6,dcsfitm_df.WEEK_OUT_OF_STOCKS_7,\
        dcsfitm_df.WEEK_OUT_OF_STOCKS_8,dcsfitm_df.WEEK_OUT_OF_STOCKS_9,dcsfitm_df.WEEK_OUT_OF_STOCKS_10,dcsfitm_df.WEEK_OUT_OF_STOCKS_11,dcsfitm_df.WEEK_OUT_OF_STOCKS_12,dcsfitm_df.WEEK_OUT_OF_STOCKS_13,\
        dcsfadm_df.DISTR_QTY_TYPE_1,dcsfadm_df.DISTR_QTY_AMOUNT_1,dcsfadm_df.DISTR_QTY_TYPE_2,dcsfadm_df.DISTR_QTY_AMOUNT_2)
    item_inventory_daily_snpsht_fct_df.write.format('delta').mode("overwrite").save(f"{default_lakehouse_path}/item_inventory_daily_snpsht_fct")


# In[ ]:


# Drop the existing table (if it exists)


# In[ ]:


# gxtobia/DCSIP-450/7-May
#jxfitzw added date handling and formatting
if STAGING_TABLE == 'purchase_order_received_fct':
    from pyspark.sql.functions import col, lit, from_unixtime, unix_timestamp, to_date, date_add, floor,to_str, concat, when, date_format, desc
    from pyspark.sql.column import cast
    from pyspark.sql.types import StringType, IntegerType
    spark.sql("DROP TABLE IF EXISTS purchase_order_received_fct")
    dcsfpod_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfpod_curated")
    dcsfpoh_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfpoh_curated")
    #new_column_names = ["DIV_SID","VNDR_SID","ITEM_SID","DT_SID","PO_NBR","PO_LEVEL","PO_LINE_NBR","PO_LINE_NBR_SUFFIX","PO_KEY_STATUS","KEY_DT_SID","SHIPTO_DIVISION","SHIPTO_WHSE","BUYER_IDX","DEPT_CODE","TOTAL_BOH","TOTAL_ON_ORDER","TOTAL_CASES_PRCHSD","TOTAL_CASES_RCVD","TOTAL_WEIGHT_RCVD","RECEIVED_DT_SID","RECEIVED_HHMM","PO_STATUS_CODE","DELIVERY_DUE_DT_SID","PO_STATUS_CODE","ORDERED_DT_SID","CASES_AVAILABLE","WTD_MOVEMENT","WTD_STOCKOUT","ACTUAL_ORDER_TYPE_1","ACTUAL_ORDER_QTY_1","INVENTORY_COST","INVOICE_COST","INVOICE_COST_CODE_1","AMOUNT_1","AMOUNT_TYPE_CODE_1","INVENTORY_COST_CODE_1","BOH_AT_RCVNG","EXCEPTION_CASES","BACKORDER_RCVNG_CODE","GALLONS_PER_CASE","NEXT_COST","LAST_RECEIVED_COST","DEAL_NUMBER","DEAL_ON_ORDER_QTY","ORIG_PO_BRACKET","ITEM_LIST_COST","DISPERSED_FLAG"]

    purchase_order_received_fct = dcsfpod_df.join(dcsfpoh_df, ((dcsfpod_df.PO_NBR ==  dcsfpoh_df.PO_NBR) & (dcsfpod_df.PO_LEVEL ==  dcsfpoh_df.PO_LEVEL)), how="inner")\
    .select(dcsfpod_df.DIV_ID,dcsfpod_df.VENDOR_NUMBER,dcsfpod_df.ITEM_NUMBER,\
    date_format(when(date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000)).otherwise(to_date(lit("2999-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('DT_SID'),\
    dcsfpod_df.PO_NBR,dcsfpod_df.PO_LEVEL,dcsfpod_df.PO_LINE_NBR,dcsfpod_df.PO_LINE_NBR_SUFFIX,\
    dcsfpod_df.PO_KEY_STATUS,\
    date_format(when(date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpod_df.KEY_DATE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.KEY_DATE_YYDDD%1000)).otherwise(to_date(lit("2999-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('KEY_DATE_YYDDD'),\
    dcsfpod_df.SHIPTO_DIVISION,dcsfpod_df.SHIPTO_WHSE,\
    dcsfpod_df.BUYER_IDX,dcsfpod_df.DEPT_CODE,dcsfpod_df.TOTAL_BOH,dcsfpod_df.TOTAL_ON_ORDER,\
    dcsfpod_df.TOTAL_CASES_PRCHSD,dcsfpod_df.TOTAL_CASES_RCVD,dcsfpod_df.TOTAL_WEIGHT_RCVD,\
    date_format(when(date_add(to_date(concat(floor(dcsfpod_df.RCVD_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.RCVD_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpod_df.RCVD_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpod_df.RCVD_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('RCVD_YYDDD'),\
    dcsfpod_df.RCVD_HHMM,dcsfpoh_df.PO_STATUS_CODE,\
    date_format(when(date_add(to_date(concat(floor(dcsfpoh_df.DELIVERY_DUE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.DELIVERY_DUE_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpoh_df.DELIVERY_DUE_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.DELIVERY_DUE_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('DELIVERY_DUE_YYDDD'),\
    dcsfpoh_df.DELIVERY_DUE_HHMM,\
    date_format(when(date_add(to_date(concat(floor(dcsfpoh_df.ORDERED_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.ORDERED_YYDDD%1000).between(to_date(lit("1997-01-01")),to_date(lit("2049-12-31"))),\
    date_add(to_date(concat(floor(dcsfpoh_df.ORDERED_YYDDD/1000).cast(StringType()) , lit("-01-01"))),dcsfpoh_df.ORDERED_YYDDD%1000)).otherwise(to_date(lit("1900-01-01"))),'yyyyMMdd').cast(IntegerType()).alias('ORDERED_YYDDD'),\
    dcsfpod_df.CASES_AVAILABLE,dcsfpod_df.WTD_MOVEMENT,dcsfpod_df.WTD_STOCKOUT,\
    dcsfpod_df.ACTUAL_ORDER_TYPE_1,dcsfpod_df.ACTUAL_ORDER_QTY_1,dcsfpod_df.INVENTORY_COST,dcsfpod_df.INVOICE_COST,\
    dcsfpoh_df.INVOICE_COST_CODE_1,dcsfpoh_df.AMOUNT_1,\
    dcsfpoh_df.AMOUNT_TYPE_CODE_1,dcsfpoh_df.INVENTORY_COST_CODE_1,\
    dcsfpod_df.BOH_AT_RCVNG,dcsfpod_df.EXCEPTION_CASES,dcsfpod_df.BACKORDER_RCVNG_CODE,dcsfpod_df.GALLONS_PER_CASE,\
    dcsfpod_df.NEXT_COST,dcsfpod_df.LAST_RECEIVED_COST,dcsfpod_df.DEAL_NUMBER,dcsfpod_df.DEAL_ON_ORDER_QTY,\
    dcsfpod_df.ORIG_PO_BRACKET,dcsfpod_df.ITEM_LIST_COST,dcsfpod_df.DISPERSED_FLAG,dcsfpod_df.IS_CURRENT)

    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DIV_ID","DIV_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("VENDOR_NUMBER","VNDR_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ITEM_NUMBER","ITEM_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_NBR","PO_NBR")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_LEVEL","PO_LEVEL")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_LINE_NBR","PO_LINE_NBR")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_LINE_NBR_SUFFIX","PO_LINE_NBR_SUFFIX")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_KEY_STATUS","PO_KEY_STATUS")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("KEY_DATE_YYDDD","KEY_DT_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("SHIPTO_DIVISION","SHIPTO_DIVISION")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("SHIPTO_WHSE","SHIPTO_WHSE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("BUYER_IDX","BUYER_IDX")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DEPT_CODE","DEPT_CODE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("TOTAL_BOH","TOTAL_BOH")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("TOTAL_ON_ORDER","TOTAL_ON_ORDER")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("TOTAL_CASES_PRCHSD","TOTAL_CASES_PRCHSD")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("TOTAL_CASES_RCVD","TOTAL_CASES_RCVD")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("TOTAL_WEIGHT_RCVD","TOTAL_WEIGHT_RCVD")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("RCVD_YYDDD","RECEIVED_DT_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("RCVD_HHMM","RECEIVED_HHMM")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("PO_STATUS_CODE","PO_STATUS_CODE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DELIVERY_DUE_YYDDD","DELIVERY_DUE_DT_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ORDERED_YYDDD","ORDERED_DT_SID")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("CASES_AVAILABLE","CASES_AVAILABLE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("WTD_MOVEMENT","WTD_MOVEMENT")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("WTD_STOCKOUT","WTD_STOCKOUT")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ACTUAL_ORDER_TYPE_1","ACTUAL_ORDER_TYPE_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ACTUAL_ORDER_QTY_1","ACTUAL_ORDER_QTY_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("INVENTORY_COST","INVENTORY_COST")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("INVOICE_COST","INVOICE_COST")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("INVOICE_COST_CODE_1","INVOICE_COST_CODE_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("AMOUNT_1","AMOUNT_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("AMOUNT_TYPE_CODE_1","AMOUNT_TYPE_CODE_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("INVENTORY_COST_CODE_1","INVENTORY_COST_CODE_1")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("BOH_AT_RCVNG","BOH_AT_RCVNG")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("EXCEPTION_CASES","EXCEPTION_CASES")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("BACKORDER_RCVNG_CODE","BACKORDER_RCVNG_CODE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("GALLONS_PER_CASE","GALLONS_PER_CASE")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("NEXT_COST","NEXT_COST")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("LAST_RECEIVED_COST","LAST_RECEIVED_COST")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DEAL_NUMBER","DEAL_NUMBER")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DEAL_ON_ORDER_QTY","DEAL_ON_ORDER_QTY")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ORIG_PO_BRACKET","ORIG_PO_BRACKET")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("ITEM_LIST_COST","ITEM_LIST_COST")
    purchase_order_received_fct = purchase_order_received_fct.withColumnRenamed("DISPERSED_FLAG","DISPERSED_FLAG")
    purchase_order_received_fct = purchase_order_received_fct.orderBy(desc("KEY_DT_SID")).dropDuplicates(["PO_NBR","PO_LINE_NBR","DIV_SID"])
    purchase_order_received_fct = purchase_order_received_fct.filter((col('PO_STATUS_CODE').isin(['C','Z','Q','S','T','U','V','W','X','Y'])))
    #purchase_order_received_fct = purchase_order_received_fct.select(dcsfpod_df['DIV_ID' ,'ITEM_NUMBER' ,'VENDOR_NUMBER' ,'KEY_DATE_YYDDD' ,'PO_NBR' ,'PO_LEVEL' ,'PO_LINE_NBR' ,'PO_LINE_NBR_SUFFIX' ,'PO_KEY_STATUS' ,'KEY_DATE_YYDDD' ,'SHIPTO_DIVISION' ,'SHIPTO_WHSE' ,'BUYER_IDX' ,'DEPT_CODE' ,'TOTAL_BOH' ,'TOTAL_ON_ORDER' ,'TOTAL_CASES_PRCHSD' ,'TOTAL_CASES_RCVD' ,'TOTAL_WEIGHT_RCVD' ,'RECEIVED_DT_SID' ,'RECEIVED_HHMM'],dcsfpoh_df['PO_STATUS_CODE','DELIVERY_DUE_DT_SID','PO_STATUS_CODE','ORDERED_DT_SID','CASES_AVAILABLE','WTD_MOVEMENT','WTD_STOCKOUT','ACTUAL_ORDER_TYPE_1','ACTUAL_ORDER_QTY_1','INVENTORY_COST','INVOICE_COST','INVOICE_COST_CODE_1','AMOUNT_1','AMOUNT_TYPE_CODE_1','INVENTORY_COST_CODE_1','BOH_AT_RCVNG','EXCEPTION_CASES','BACKORDER_RCVNG_CODE','GALLONS_PER_CASE','NEXT_COST','LAST_RECEIVED_COST','DEAL_NUMBER','DEAL_ON_ORDER_QTY','ORIG_PO_BRACKET','ITEM_LIST_COST','DISPERSED_FLAG'])
    purchase_order_received_fct.write.mode("overwrite").option("overwriteSchema", "true").format('delta').save(f"{default_lakehouse_path}/purchase_order_received_fct")


# In[ ]:


if STAGING_TABLE == 'item_dim':
    df = spark.read.load(f"{default_lakehouse_path}/item_dim")
    df1 = df.dropDuplicates(["ITEM_SID"])
    df1.write.format("delta").mode("overwrite").save(f"{default_lakehouse_path}/item_dim")


# In[ ]:


# jmerics/DCSIP-450/7-May
if STAGING_TABLE == 'item_inventory_hourly_snpsht_fct':
    dcsfitm_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfitm_curated")
    dcsfadm_df = spark.read.load(f"{default_lakehouse_path}/vsam_dcsfadm_curated")
    load_ts = dcsfitm_df.filter(col('EFFECTIVE_START_DATE') < '2400-01-01').agg(max('EFFECTIVE_START_DATE')).collect()[0][0]
    item_inventory_hourly_snpsht_fct_df = dcsfitm_df.where( \
                                                        (col('EFFECTIVE_START_DATE') == load_ts) | \
                                                        (col('EFFECTIVE_END_DATE') == load_ts) \
                                                        )
    item_inventory_hourly_snpsht_fct_df = item_inventory_hourly_snpsht_fct_df.join( \
        dcsfadm_df, \
        ((dcsfitm_df.DIV_ID == dcsfadm_df.DIV_ID) & \
        (dcsfitm_df.RECORD_KEY == dcsfadm_df.ITEM_NUMBER) & \
        (dcsfitm_df.IS_CURRENT == True) & \
        (dcsfadm_df.IS_CURRENT == True)), \
        how="left").select(dcsfitm_df.DIV_ID, \
                        dcsfitm_df.RECORD_KEY, \
                        dcsfitm_df.VENDOR_NBR, \
                        dcsfitm_df.AVG_WKLY_MVMNT, \
                        dcsfitm_df.TOTAL_BOH, \
                        dcsfitm_df.D_PRODUCT_ON_HAND, \
                        dcsfitm_df.WTD_STOCKOUT, \
                        dcsfitm_df.BUYING_SYSTEM_FL, \
                        dcsfitm_df.TEMP_OUT_OF_STOCK_FL, \
                        dcsfitm_df.NEXT_DUE_CASES, \
                        dcsfitm_df.ITEMNOFULL, \
                        dcsfitm_df.SHIP_CASE_CUBE, \
                        dcsfitm_df.PICK_SLOT_NUMBER_MSC, \
                        dcsfitm_df.PICK_SLOT_WHSE, \
                        dcsfitm_df.REPL_ITM, \
                        dcsfitm_df.VENDOR_CASE_CUBE, \
                        dcsfitm_df.BUYING_MULTIPLE, \
                        dcsfitm_df.MULTIPLE_W_V_FLAG, \
                        dcsfitm_df.MULTIPLE_P_T_FLAG, \
                        dcsfitm_df.MULTIPLE_MULTIPLIER, \
                        dcsfitm_df.TI, \
                        dcsfitm_df.HI, \
                        dcsfitm_df.REPACK_FACTOR, \
                        dcsfitm_df.VENDOR_TI, \
                        dcsfitm_df.VENDOR_HI, \
                        dcsfitm_df.TOTAL_DUE_IN_CASES, \
                        dcsfitm_df.ITEMNOUNIT, \
                        dcsfitm_df.SHIP_CASE_WEIGHT, \
                        dcsfitm_df.TERMINATION_DATE, \
                        dcsfitm_df.VENDOR_CASE_WEIGHT, \
                        dcsfitm_df.YTD_DISTRIBUTION, \
                        dcsfitm_df.YTD_MOVEMENT, \
                        dcsfitm_df.YTD_OUT_OF_STOCKS, \
                        dcsfitm_df.WTD_DISTRIBUTION, \
                        dcsfitm_df.WTD_MOVEMENT, \
                        dcsfitm_df.WEEK_DISTRIBUTION_1, \
                        dcsfitm_df.WEEK_DISTRIBUTION_2, \
                        dcsfitm_df.WEEK_DISTRIBUTION_3, \
                        dcsfitm_df.WEEK_DISTRIBUTION_4, \
                        dcsfitm_df.WEEK_DISTRIBUTION_5, \
                        dcsfitm_df.WEEK_DISTRIBUTION_6, \
                        dcsfitm_df.WEEK_DISTRIBUTION_7, \
                        dcsfitm_df.WEEK_DISTRIBUTION_8, \
                        dcsfitm_df.WEEK_DISTRIBUTION_9, \
                        dcsfitm_df.WEEK_DISTRIBUTION_10, \
                        dcsfitm_df.WEEK_DISTRIBUTION_11, \
                        dcsfitm_df.WEEK_DISTRIBUTION_12, \
                        dcsfitm_df.WEEK_DISTRIBUTION_13, \
                        dcsfitm_df.WEEK_MOVEMENT_1, \
                        dcsfitm_df.WEEK_MOVEMENT_2, \
                        dcsfitm_df.WEEK_MOVEMENT_3, \
                        dcsfitm_df.WEEK_MOVEMENT_4, \
                        dcsfitm_df.WEEK_MOVEMENT_5, \
                        dcsfitm_df.WEEK_MOVEMENT_6, \
                        dcsfitm_df.WEEK_MOVEMENT_7, \
                        dcsfitm_df.WEEK_MOVEMENT_8, \
                        dcsfitm_df.WEEK_MOVEMENT_9, \
                        dcsfitm_df.WEEK_MOVEMENT_10, \
                        dcsfitm_df.WEEK_MOVEMENT_11, \
                        dcsfitm_df.WEEK_MOVEMENT_12, \
                        dcsfitm_df.WEEK_MOVEMENT_13, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_1, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_2, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_3, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_4, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_5, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_6, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_7, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_8, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_9, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_10, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_11, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_12, \
                        dcsfitm_df.WEEK_OUT_OF_STOCKS_13, \
                        dcsfadm_df.DISTR_QTY_TYPE_1, \
                        dcsfadm_df.DISTR_QTY_AMOUNT_1, \
                        dcsfadm_df.DISTR_QTY_TYPE_2, \
                        dcsfadm_df.DISTR_QTY_AMOUNT_2)
    item_inventory_hourly_snpsht_fct_df = item_inventory_hourly_snpsht_fct_df.withColumn('LOAD_TS', lit(load_ts))
    item_inventory_hourly_snpsht_fct_df.write.mode('overwrite').format('delta').save(f"{default_lakehouse_path}/item_inventory_hourly_snpsht_fct")


# In[ ]:


# jmavila dims
# delta_df = spark.read.table("edw_mc_wd1c_cust_curated")
# delta_df.write.format('delta').mode("overwrite").saveAsTable("customer_dim")
# divsion_item_department_dim =edw_mc_wd4d_div_item_dept_curated  - last 4 ok - complete
# div_item_df=spark.read.table("edw_mc_wd4d_div_item_dept_curated")
# div_item_df.write.format('delta').mode("overwrite").saveAsTable("divsion_item_department_dim")
# division_vendor_dim = edw_mc_wd2v_div_vndr_curated - last 4 ok - complete
# div_vendor_df=spark.read.table("edw_mc_wd2v_div_vndr_curated")
# div_vendor_df.write.format('delta').mode("overwrite").saveAsTable("division_vendor_dim")
# invoice_header_dim = edw_mc_wd2i_invc_hdr_curated - last 4 ok - complete
# inv_hdr_df=spark.read.table("edw_mc_wd2i_invc_hdr_curated")
# inv_hdr_df.write.format('delta').mode("overwrite").saveAsTable("invoice_header_dim")
# item_dim=edw_mc_wd1i_item_curated - last 4 not ok
# itm_df = spark.read.table("edw_mc_wd1i_item_curated") 
# itm_df.write.formt('delta').mode("overwrite").saveAsTable("item_dim")
# ship_mode_dim=edw_mc_wd1l_shp_mode_curated -  last 4 ok - complete
# ship_mode_df=spark.read.table("edw_mc_wd1l_shp_mode_curated")
# ship_mode_df.write.format('delta').mode("overwrite").saveAsTable("ship_mode_dim")

