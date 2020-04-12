# Databricks notebook source
df = sqlContext.sql("SELECT * FROM covid_data")

# COMMAND ----------

df.take(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from covid_data
# MAGIC limit 30;

# COMMAND ----------

df1 = sqlContext.sql("SELECT ProvinceState,CountryRegion,Confirmed FROM covid_data")

# COMMAND ----------

df1.take(30)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ProvinceState,CountryRegion,Confirmed FROM covid_data
# MAGIC limit 30;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ProvinceState,CountryRegion,sum(Confirmed) as TotalCases FROM covid_data
# MAGIC group by CountryRegion,ProvinceState
# MAGIC order by TotalCases desc
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CountryRegion,sum(confirmed) as TotalCases,sum(Deaths),sum(Recovered) FROM covid_data
# MAGIC group by CountryRegion
# MAGIC order by TotalCases desc
# MAGIC limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ObservationDate,sum(confirmed) as MaxCases FROM covid_data
# MAGIC group by ObservationDate
# MAGIC order by MaxCases desc
# MAGIC limit 10;

# COMMAND ----------


