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

# 📌 Stap 1: Imports
from pyspark.sql.functions import from_json, col, to_date, explode
from pyspark.sql.types import MapType, StringType
from datetime import datetime, timedelta
import re

# 📌 Stap 2: Laad CSV-bestanden
df = spark.read.option("header", True) \
    .option("delimiter", ",") \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "PERMISSIVE") \
    .option("recursiveFileLookup", "true") \
    .csv("Files/azurecostexports_1/")

# 📌 Stap 3: Filter alleen gisteren
gisteren = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
df = df.withColumn("parsed_date", to_date(col("date"), "MM/dd/yyyy"))
df = df.filter(col("parsed_date") == gisteren)

# 📌 Stap 4: Parse 'tags'
df = df.withColumn("tags_json", from_json(col("tags"), MapType(StringType(), StringType())))

# 📌 Stap 5: Voeg extra kolommen toe vanuit tags_json
all_keys = (
    df.select(explode(col("tags_json")).alias("key", "value"))
    .select("key")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)
for key in all_keys:
    if len(key) <= 120 and not key.startswith("hidden-link:"):
        safe_key = key.replace(" ", "_").replace(".", "_").replace("-", "_")[:120]
        df = df.withColumn(safe_key, col("tags_json").getItem(key))

# 📌 Stap 6: Hernoem kolommen veilig
renamed_columns = []
seen_names = set()
for col_name in df.columns:
    safe_name = re.sub(r'\W+', '_', col_name.strip())[:120]
    while safe_name in seen_names:
        safe_name = safe_name[:-1]
    renamed_columns.append(safe_name)
    seen_names.add(safe_name)
df = df.toDF(*renamed_columns)

# 📌 Stap 7: Verwijder onbruikbare kolommen
df = df.select([col_name for col_name in df.columns if "hidden_link" not in col_name])
if "tags_json" in df.columns:
    df = df.drop("tags_json")

# 📌 Stap 8: Append naar azurecostexport_junisubset
if df.count() > 0:
    df.write.mode("append") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable("azurecostexport_junisubset")
else:
    print("Geen nieuwe rijen gevonden voor gisteren.")

# 📌 Stap 9: Filter relevante kolommen voor uiteindelijke view
df_filtered = spark.sql(f"""
SELECT
   servicePeriodStartDate,
   date,
   subscriptionName,
   SubscriptionId,
   costInBillingCurrency,
   Debiteurnummer,
   BusinessUnit,
   resourceGroupName,
   parsed_date
FROM azurecostexport_junisubset
WHERE parsed_date = '{gisteren}'
""")

# 📌 Stap 10: Voeg toe aan bram_view_exported
if df_filtered.count() > 0:
    df_filtered.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable("bram_view_exported")
else:
    print("Geen nieuwe gefilterde rijen voor bram_view_exported.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
