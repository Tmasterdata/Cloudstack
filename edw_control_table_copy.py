#!/usr/bin/env python
# coding: utf-8

# ## edw_control_table_copy
# 
# New notebook

# In[1]:


spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")


# In[3]:


from pyspark.sql.functions import lit, col
from pyspark.sql import Row

prod_table = 'abfss://af50e063-fb6c-4a72-ac47-97306bc62af9@onelake.dfs.fabric.microsoft.com/7d3bc150-7ea5-4154-acc8-c27058bea456/Tables/edw_control_table'
test_table = 'abfss://ace08d22-80c7-469c-8077-66c1439bb17a@onelake.dfs.fabric.microsoft.com/5f4cdfd1-5208-47f7-aee2-f6d7d08bf3f8/Tables/edw_control_table'
dev_table = 'abfss://314b45bf-7cc4-4691-95c9-a76783a015f8@onelake.dfs.fabric.microsoft.com/6d196d6a-5a8b-4676-8e19-e7a7742525c0/Tables/edw_control_table'
edw_control = spark.read.load(dev_table)
edw_control_prod = edw_control.withColumn('TargetDestinationWorkspace', lit('af50e063-fb6c-4a72-ac47-97306bc62af9')) \
                              .withColumn('TargetDestinationLakehouse', lit('7d3bc150-7ea5-4154-acc8-c27058bea456'))
# edw_control_prod = edw_control.select('*', lit('af50e063-fb6c-4a72-ac47-97306bc62af9').alias('TargetDestinationWorkspace'),  lit('7d3bc150-7ea5-4154-acc8-c27058bea456').alias('TargetDestinationLakehouse'))
display(edw_control_prod)

# schema = edw_control_prod.schema
# template_row_df = edw_control_prod.orderBy(col('PipelineId').desc()).limit(1)

# # Collect the last row as a dictionary
# template_row = template_row_df.collect()[0].asDict()
# new_rows = []

# jc1g_row = template_row.copy()
# jc1g_row['PipelineId'] = 30
# jc1g_row['SourceTable'] = 'edw_mc_jc1g_crp_acct_grp_curated'
# jc1g_row['PrimaryKey'] = 'JC1G_CRP_ACCT_GRP_SID'
# jc1g_row['TargetTable'] = 'corp_account_group_dim'
# new_rows.append(Row(**jc1g_row))

# wd8c_row = template_row.copy()
# wd8c_row['PipelineId'] = 31
# wd8c_row['SourceTable'] = 'edw_mc_wd8c_cmbin_crp_tbl_curated'
# wd8c_row['PrimaryKey'] = 'WD8C_CMBIN_CRP_SID'
# wd8c_row['TargetTable'] = 'combined_corp_dim'
# new_rows.append(Row(**wd8c_row))

# jc1c_row = template_row.copy()
# jc1c_row['PipelineId'] = 32
# jc1c_row['SourceTable'] = 'edw_mc_jc1c_cntl_crp_acct_curated'
# jc1c_row['PrimaryKey'] = 'JC1C_CNTL_CRP_ACCT_SID'
# jc1c_row['TargetTable'] = 'control_corp_account_dim'
# new_rows.append(Row(**jc1c_row))

# additional_df = spark.createDataFrame(new_rows, schema=schema)
# additional_df = additional_df.select(edw_control_prod.columns)
# combined_df = edw_control_prod.unionByName(additional_df)
# display(combined_df)

# combined_df.write.format('delta').mode('overwrite').save(prod_table)

