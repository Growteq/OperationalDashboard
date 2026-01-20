# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

df = (
    spark.read.option("header", True)
    .option("delimiter", ",")
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .option("recursiveFileLookup", "true")
    .csv("abfss://d183c24c-5af7-4637-acfd-a273cbc9ba49@onelake.dfs.fabric.microsoft.com/333440f7-e390-4f80-856b-8eeb1f8bfd78/Files/azurecostexports_1/001cc419-72bc-4fa5-99da-66c750c919be/DailyCostExport-001cc419-72bc-4fa5-99da-66c750c919be/20250301-20250331/DailyCostExport-001cc419-72bc-4fa5-99da-66c750c919be_07031dec-7843-4ca9-810c-ec6d3fd86d29.csv")
)

display(df.limit(100))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# 📌 Step 1: Imports
# =====================================================
from pyspark.sql.functions import (
    from_json, col, to_date, explode, input_file_name,
    regexp_extract, lit
)
from pyspark.sql.types import MapType, StringType
from pyspark.sql import Row, functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar, re, time
from py4j.protocol import Py4JJavaError

# =====================================================
# 📌 Step 2: Accept parameter (optional)
# =====================================================
try:
    run_date = dbutils.widgets.get("run_date")
    print(f"📅 Using provided run_date: {run_date}")
except:
    run_date = None

if not run_date or run_date.strip() == "":
    run_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"📅 Defaulted to yesterday: {run_date}")

# =====================================================
# 📌 Step 3: Derive month folder for run_date
# =====================================================
run_dt = datetime.strptime(run_date, "%Y-%m-%d")
month_start = run_dt.replace(day=1)
month_end_day = calendar.monthrange(run_dt.year, run_dt.month)[1]
month_end = run_dt.replace(day=month_end_day)
month_folder = f"{month_start.strftime('%Y%m%d')}-{month_end.strftime('%Y%m%d')}"
print(f"📂 Target month folder: {month_folder}")

# =====================================================
# 📌 Step 4: Build optimized path for that month
# =====================================================
base_path = (
    f"abfss://d183c24c-5af7-4637-acfd-a273cbc9ba49@onelake.dfs.fabric.microsoft.com/"
    f"333440f7-e390-4f80-856b-8eeb1f8bfd78/Files/azurecostexports_1/*/*/{month_folder}/"
)
print(f"📁 Reading from: {base_path}")

# =====================================================
# 📌 Step 5: Read only that month’s CSVs
# =====================================================
t0 = time.time()
df = (
    spark.read.option("header", True)
    .option("delimiter", ",")
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(base_path)
)
print(f"✅ Loaded data in {time.time()-t0:.1f}s")

# =====================================================
# 📌 Step 5b: Capture source file and folder lineage
# =====================================================
df = df.withColumn("SourcePath", input_file_name())
df = (
    df.withColumn("ExportId", regexp_extract(col("SourcePath"), r"/azurecostexports_1/([^/]+)/", 1))
      .withColumn("ExportName", regexp_extract(col("SourcePath"), r"/([^/]+)/\d{8}-\d{8}/", 1))
      .withColumn("MonthFolder", regexp_extract(col("SourcePath"), r"/(\d{8}-\d{8})/", 1))
      .withColumn("FileName", regexp_extract(col("SourcePath"), r"([^/]+)$", 1))
)
# =====================================================
# 📌 Step 5c: Schema dictionary & drift detection (final first-run safe)
# =====================================================
schema_dict_path = (
    "abfss://d183c24c-5af7-4637-acfd-a273cbc9ba49@onelake.dfs.fabric.microsoft.com/"
    "333440f7-e390-4f80-856b-8eeb1f8bfd78/Tables/azurecostexports_schema_dict"
)

current_schema = [(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields]
current_schema_df = spark.createDataFrame(
    [Row(ColumnName=c[0], DataType=c[1], Nullable=c[2]) for c in current_schema]
).withColumn("RunDate", lit(run_date)).withColumn("CapturedAt", lit(datetime.utcnow().isoformat()))

start = time.time()

# ✅ 1️⃣ Check if the directory physically exists
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
schema_dir_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(schema_dict_path))

if not schema_dir_exists:
    print("📁 No existing schema dictionary folder found. Creating new Delta table (Version 1).")
    current_schema_df = current_schema_df.withColumn("Version", lit(1))
    (
        current_schema_df.write
        .format("delta")
        .mode("overwrite")
        .save(schema_dict_path)
    )
    print("📘 Created new schema dictionary table (Version 1).")
else:
    print("📚 Existing schema dictionary found. Checking for changes...")
    existing_schema_df = spark.read.format("delta").load(schema_dict_path)
    latest_schema = existing_schema_df.orderBy(F.desc("CapturedAt")).limit(1000).collect()

    version = (
        existing_schema_df.select(F.max("Version")).collect()[0][0]
        if "Version" in existing_schema_df.columns
        else 1
    )
    existing_cols = {r.ColumnName.lower(): r.DataType for r in latest_schema}
    current_cols = {c[0].lower(): c[1] for c in current_schema}

    added   = set(current_cols) - set(existing_cols)
    removed = set(existing_cols) - set(current_cols)
    changed = {k for k,v in current_cols.items() if k in existing_cols and v != existing_cols[k]}

    if added or removed or changed:
        version += 1
        current_schema_df = current_schema_df.withColumn("Version", lit(version))
        current_schema_df.write.format("delta").mode("append").save(schema_dict_path)
        print(f"⚠️ Schema change detected → updated to Version {version}")
        print(f"   ➕ Added: {added}\n   ➖ Removed: {removed}\n   🔁 Changed: {changed}")
    else:
        print(f"✅ No schema changes detected (Version {version}).")

print(f"⏱️ Schema check completed in {time.time()-start:.1f}s")

# =====================================================
# 📌 Step 6: Parse & filter
# =====================================================
df = df.withColumn("parsed_date", to_date(col("date"), "MM/dd/yyyy"))
df = df.filter(col("parsed_date") == run_date)

# =====================================================
# 📌 Step 7: Parse tags
# =====================================================
df = df.withColumn("tags_json", from_json(col("tags"), MapType(StringType(), StringType())))
all_keys = (
    df.select(explode(col("tags_json")).alias("key", "value"))
    .select("key").distinct().rdd.flatMap(lambda x: x).collect()
)
for key in all_keys:
    if len(key) <= 120 and not key.startswith("hidden-link:"):
        safe_key = re.sub(r'\W+', '_', key.strip())[:120]
        df = df.withColumn(safe_key, col("tags_json").getItem(key))

# =====================================================
# 📌 Step 8: Clean up
# =====================================================
df = df.toDF(*[re.sub(r'\W+', '_', c.strip())[:120] for c in df.columns])
df = df.select([c for c in df.columns if "hidden_link" not in c])
if "tags_json" in df.columns:
    df = df.drop("tags_json")

# =====================================================
# 📌 Step 9: Idempotent Delta write (safe for re-runs)
# =====================================================
target_path = (
    "abfss://d183c24c-5af7-4637-acfd-a273cbc9ba49@onelake.dfs.fabric.microsoft.com/"
    "333440f7-e390-4f80-856b-8eeb1f8bfd78/Tables/azurecostexports_all"
)

# Safe delete of existing partition for this date
try:
    spark.sql(f"DELETE FROM delta.`{target_path}` WHERE parsed_date = '{run_date}'")
    print(f"🧹 Deleted existing records for {run_date}")
except Exception as e:
    if "Path does not exist" in str(e):
        print("📁 Target table not found yet — it will be created.")
    else:
        print(f"⚠️ Skipped delete step: {e}")

if df.count() > 0:
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("parsed_date")
        .save(target_path)
    )
    print(f"✅ {df.count()} records appended for {run_date}")
else:
    print(f"ℹ️ No data found for {run_date}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# 📌 Step 5c: Schema dictionary & drift detection (final first-run safe)
# =====================================================
schema_dict_path = (
    "abfss://d183c24c-5af7-4637-acfd-a273cbc9ba49@onelake.dfs.fabric.microsoft.com/"
    "333440f7-e390-4f80-856b-8eeb1f8bfd78/Tables/azurecostexports_schema_dict"
)

current_schema = [(f.name, f.dataType.simpleString(), f.nullable) for f in df.schema.fields]
current_schema_df = spark.createDataFrame(
    [Row(ColumnName=c[0], DataType=c[1], Nullable=c[2]) for c in current_schema]
).withColumn("RunDate", lit(run_date)).withColumn("CapturedAt", lit(datetime.utcnow().isoformat()))

start = time.time()

# ✅ 1️⃣ Check if the directory physically exists
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
schema_dir_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(schema_dict_path))

if not schema_dir_exists:
    print("📁 No existing schema dictionary folder found. Creating new Delta table (Version 1).")
    current_schema_df = current_schema_df.withColumn("Version", lit(1))
    (
        current_schema_df.write
        .format("delta")
        .mode("overwrite")
        .save(schema_dict_path)
    )
    print("📘 Created new schema dictionary table (Version 1).")
else:
    print("📚 Existing schema dictionary found. Checking for changes...")
    existing_schema_df = spark.read.format("delta").load(schema_dict_path)
    latest_schema = existing_schema_df.orderBy(F.desc("CapturedAt")).limit(1000).collect()

    version = (
        existing_schema_df.select(F.max("Version")).collect()[0][0]
        if "Version" in existing_schema_df.columns
        else 1
    )
    existing_cols = {r.ColumnName.lower(): r.DataType for r in latest_schema}
    current_cols = {c[0].lower(): c[1] for c in current_schema}

    added   = set(current_cols) - set(existing_cols)
    removed = set(existing_cols) - set(current_cols)
    changed = {k for k,v in current_cols.items() if k in existing_cols and v != existing_cols[k]}

    if added or removed or changed:
        version += 1
        current_schema_df = current_schema_df.withColumn("Version", lit(version))
        current_schema_df.write.format("delta").mode("append").save(schema_dict_path)
        print(f"⚠️ Schema change detected → updated to Version {version}")
        print(f"   ➕ Added: {added}\n   ➖ Removed: {removed}\n   🔁 Changed: {changed}")
    else:
        print(f"✅ No schema changes detected (Version {version}).")

print(f"⏱️ Schema check completed in {time.time()-start:.1f}s")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark_df = spark.read.table(f"azurecostexports_all")

max_rows_to_read = 1000
spark_df = spark_df.limit(max_rows_to_read)


display(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
