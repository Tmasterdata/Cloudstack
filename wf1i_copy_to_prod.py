#!/usr/bin/env python
# coding: utf-8

# ## wf1i_copy_to_prod
# 
# New notebook

# In[ ]:


spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.yarn.max.executor.failures", "50000")
spark.conf.set("spark.sql.shuffle.partitions", "5000")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")


# In[ ]:


from pyspark.sql.functions import col

prod_curated = 'abfss://954ef8c6-be2c-4fdc-9527-184f23e0fb5a@onelake.dfs.fabric.microsoft.com/94ba37b9-6259-473b-9ae0-341626940f37/Tables/edw_mc_wf1i_invc_sls_dtl_curated'
dev_curated = 'abfss://95c5091f-9302-443e-98af-1eb537a63d27@onelake.dfs.fabric.microsoft.com/f33e1408-be53-4ec1-8946-4bedfdec1fc8/Tables/edw_mc_wf1i_invc_sls_dtl_curated'
wf1i_dev = spark.read.load(dev_curated)
wf1i_divs = wf1i_dev.select("DIV_SID").filter(~(col("DIV_SID").isin([244, 264, 235, 229, 240, 389, 360, 390, 246, 263, 365, 211, 228, 232, 361, 373, 237, 391, 227, 234, 231, 367, 375, 357, 363, 359, 387, 224, 239, 371, 241, 376, 223]))).distinct().collect()
divs_loaded = ''
for row in wf1i_divs:
    print(f"Loading {row['DIV_SID']}")
    wf1i_by_div = wf1i_dev.filter(col("DIV_SID") == row['DIV_SID'])
    wf1i_by_div.write.format('delta').mode('append').save(prod_curated)
    if divs_loaded == '':
        divs_loaded += f"{row['DIV_SID']}"
    else:
        divs_loaded += f", {row['DIV_SID']}"
    print(f'Loaded: {divs_loaded}')

