# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window

sc = spark.sparkContext
fs = (sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()))

Primary_person_df = spark.read.format('com.databricks.spark.csv').option("inferSchema", "true").options(header='true').option("delimiter", ",").load("dbfs:/mnt/rs06ue2dipadl03/BCG/Primary_Person_use.csv")

Units_df = spark.read.format('com.databricks.spark.csv').option("inferSchema", "true").options(header='true').option("delimiter", ",").load("dbfs:/mnt/rs06ue2dipadl03/BCG/Units_use.csv")

damaged_df = spark.read.format('com.databricks.spark.csv').option("inferSchema", "true").options(header='true').option("delimiter", ",").load("dbfs:/mnt/rs06ue2dipadl03/BCG/Damages_use.csv")

##### Analytics1 #############

NoOfCrashes_df = Primary_person_df.filter("PRSN_GNDR_ID = 'MALE'").selectExpr("count(CRASH_ID)")
NoOfCrashes_df.show()

# COMMAND ----------

#### Analytics3  ###############

from pyspark.sql.functions import *
StateWithHighestFemaleAccident_df = Primary_person_df.filter("PRSN_GNDR_ID = 'FEMALE'").groupBy("DRVR_LIC_STATE_ID").agg(count("CRASH_ID").alias('count')).orderBy(desc("count")).drop("count").limit(1)
StateWithHighestFemaleAccident_df.show()


# COMMAND ----------

######## Analytics 4 ####################

Top5_15_VEH_MAKE_ID_df = Units_df.groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias('count')).orderBy(desc("count")).take(15)
display(Top5_15_VEH_MAKE_ID_df[4:])

# COMMAND ----------

##### Analytics5 ################
df_stg = Units_df.join(Primary_person_df, Units_df.CRASH_ID == Primary_person_df.CRASH_ID, "inner")

df_stg2 = df_stg.groupBy(df_stg.VEH_BODY_STYL_ID,df_stg.PRSN_ETHNICITY_ID).agg(count("PRSN_ETHNICITY_ID").alias("count_PRSN_ETHNICITY_ID"))

w = Window().partitionBy(df_stg2.VEH_BODY_STYL_ID).orderBy(desc("count_PRSN_ETHNICITY_ID"))

TopEthnicUserGroup_df = df_stg2.withColumn("user_group_rank", dense_rank().over(w)).filter("user_group_rank=1").drop("count_PRSN_ETHNICITY_ID","user_group_rank").withColumnRenamed("PRSN_ETHNICITY_ID","top_ethnic_user_group")

TopEthnicUserGroup_df.show()

# COMMAND ----------

###### Analytics 6 ############

Top5ZipCodesWithHighestNumberCrashes_df = Primary_person_df.filter("PRSN_ALC_RSLT_ID = 'Positive' and DRVR_ZIP is not null").groupBy("DRVR_ZIP").agg(count("CRASH_ID").alias("count")).orderBy(desc("count")).drop("count").limit(5)
Top5ZipCodesWithHighestNumberCrashes_df.show()

# COMMAND ----------

##### Analytics 7 ############

units_stg_df = Units_df.withColumn("damage_level",(substring(Units_df.VEH_DMAG_SCL_2_ID,9,1)).cast('Int'))
# display(units_stg_df)
no_damage_property_df = units_stg_df.join(damaged_df,units_stg_df.CRASH_ID == damaged_df.CRASH_ID,"leftanti").filter("damage_level > 4").select("CRASH_ID","damage_level")
no_damage_property_df.show()


# COMMAND ----------


