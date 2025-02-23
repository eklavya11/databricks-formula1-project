# Databricks notebook source
dbutils.widgets.text("p_d_s","")
ds = dbutils.widgets.get("p_d_s")

# COMMAND ----------

dbutils.widgets.text("p_f_d","")
fd = dbutils.widgets.get("p_f_d")

# COMMAND ----------

#dbutils.notebook.help()
v_result=dbutils.notebook.run("1.Ingest_circuits_file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("2.Ingest_races_file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("3.Ingest constructors file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("4.Ingest Drivers file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("5. Ingest Results file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("6. Ingest Pit_stops file",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("7. Ingest lap_times folder",0,{"p_data_source":ds,"p_file_date":fd})

# COMMAND ----------

v_result=dbutils.notebook.run("8. Ingest qualifying folder",0,{"p_data_source":ds,"p_file_date":fd})