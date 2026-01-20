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
from pyspark.sql.functions import expr
import re

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

# 5. Voeg alleen veilige keys toe (max 120 tekens en geen 'hidden-link:')
for key in all_keys:
    if len(key) <= 120 and not key.startswith("hidden-link:"):
        safe_key = key.replace(" ", "_").replace(".", "_").replace("-", "_")[:120]
        df = df.withColumn(safe_key, col("tags_json").getItem(key))

# 6. Hernoem ALLE kolommen: veilige tekens en max 120 lang
renamed_columns = []
seen_names = set()
for col_name in df.columns:
    safe_name = re.sub(r'\W+', '_', col_name.strip())[:120]
    # Zorg dat ze uniek blijven
    while safe_name in seen_names:
        safe_name = safe_name[:-1]
    renamed_columns.append(safe_name)
    seen_names.add(safe_name)

df = df.toDF(*renamed_columns)

# 7. Extra safety: verwijder kolommen die 'hidden_link' in naam bevatten
kolommen_die_mogen_blijven = [col_name for col_name in df.columns if 'hidden_link' not in col_name]
df = df.select(*kolommen_die_mogen_blijven)

# 7b. Verwijder kolom 'tags_json' (Delta ondersteunt geen MapType)
df = df.drop("tags_json")

# 8. Schrijf naar Delta-tabel in Lakehouse
df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("AzureCostExport")

# 9. Controleer resultaat
display(spark.sql("SELECT * FROM AzureCostExport LIMIT 100"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
