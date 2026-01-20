# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "333440f7-e390-4f80-856b-8eeb1f8bfd78",
# META       "default_lakehouse_name": "Azure_kosten_Lakehouse",
# META       "default_lakehouse_workspace_id": "d183c24c-5af7-4637-acfd-a273cbc9ba49",
# META       "known_lakehouses": [
# META         {
# META           "id": "333440f7-e390-4f80-856b-8eeb1f8bfd78"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import MapType, StringType

# 1. Lees alle CSV-bestanden uit alle submappen van azurecostexports_1
df = spark.read.option("header", True) \
    .option("delimiter", ",") \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "PERMISSIVE") \
    .option("recursiveFileLookup", "true") \
    .csv("Files/azurecostexports_1/")

# 2. Bekijk de eerste rijen om te controleren of het goed is ingelezen
display(df)

# 3. Parse de kolom 'tags' als JSON
df = df.withColumn("tags_json", from_json(col("tags"), MapType(StringType(), StringType())))

# 4. Bepaal alle unieke keys in tags_json
all_keys = (
    df.selectExpr("explode(map_keys(tags_json)) as key")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

# 5. Voeg voor elke key een kolom toe met de waarde uit tags_json
for key in all_keys:
    safe_key = key.replace(" ", "_").replace(".", "_").replace("-", "_")
    df = df.withColumn(safe_key, col("tags_json").getItem(key))

# 6. Hernoem bestaande kolommen met ongeldige tekens
for col_name in df.columns:
    safe_col_name = col_name.replace(" ", "_").replace(".", "_").replace("-", "_")
    if col_name != safe_col_name:
        df = df.withColumnRenamed(col_name, safe_col_name)

# 7. Verwijder de kolom tags_json
df = df.drop("tags_json")

# 8. Schrijf de DataFrame naar de gewenste tabel in je Lakehouse, met schema-overwrite
df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("s1_pre_AzureCosts")

# 9. Controleer de tabel met een SQL-query
display(spark.sql("SELECT * FROM s1_pre_AzureCosts LIMIT 100"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("""
    SELECT *
    FROM test_lakehouse_azure.s1_pre_azurecosts
    WHERE Debiteurnummer IS NOT NULL
    LIMIT 1000
""")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
