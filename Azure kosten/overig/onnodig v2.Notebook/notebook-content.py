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

from pyspark.sql.functions import from_json, col, to_date, expr
from pyspark.sql.types import MapType, StringType
import re

# 1. Haal de laatst verwerkte datum op uit bestaande Delta-tabel
last_date_df = spark.sql("SELECT MAX(to_date(date, 'MM/dd/yyyy')) as last_date FROM AzureCostExport")
last_processed_date = last_date_df.collect()[0]['last_date']

# 2. Lees nieuwe CSV’s uit blob storage
df = spark.read.option("header", True) \
    .option("delimiter", ",") \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("mode", "PERMISSIVE") \
    .option("recursiveFileLookup", "true") \
    .csv("Files/azurecostexports_1/")

# 3. Filter alleen rijen die nieuwer zijn dan laatste verwerkte datum
df = df.withColumn("parsed_date", to_date(col("date"), "MM/dd/yyyy"))
df = df.filter(col("parsed_date") > expr(f"date('{last_processed_date}')"))

# 4. Parse de kolom 'tags' als JSON
df = df.withColumn("tags_json", from_json(col("tags"), MapType(StringType(), StringType())))

# 5. Voeg alleen veilige keys toe (max 120 tekens en geen 'hidden-link:')
all_keys = (
    df.selectExpr("explode(map_keys(tags_json)) as key")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

for key in all_keys:
    if len(key) <= 120 and not key.startswith("hidden-link:"):
        safe_key = key.replace(" ", "_").replace(".", "_").replace("-", "_")[:120]
        df = df.withColumn(safe_key, col("tags_json").getItem(key))

# 6. Hernoem ALLE kolommen: veilige tekens en max 120 lang
renamed_columns = []
seen_names = set()
for col_name in df.columns:
    safe_name = re.sub(r'\W+', '_', col_name.strip())[:120]
    while safe_name in seen_names:
        safe_name = safe_name[:-1]
    renamed_columns.append(safe_name)
    seen_names.add(safe_name)

df = df.toDF(*renamed_columns)

# 7. Verwijder 'hidden_link' kolommen en tags_json
df = df.select([c for c in df.columns if 'hidden_link' not in c and c != "tags_json"])

# 8. Schrijf toe aan Delta-tabel (append)
df.write.mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("AzureCostExport")

# 9. Optioneel: bekijk resultaat
display(spark.sql("SELECT * FROM AzureCostExport ORDER BY date DESC LIMIT 100"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
