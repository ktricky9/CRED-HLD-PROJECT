#!/bin/bash
# Helper script to display schema, count, and sample data from a Hudi table
# Arguments:
#   $1: Table path (s3a://hudi-data/orders or s3a://hudi-data/orders_transformed)
#   $2: Table name for display purposes (e.g., "Original Orders" or "Transformed Orders")

TABLE_PATH=$1
TABLE_NAME=$2

echo ""
echo "📊 DATA SUMMARY FOR: $TABLE_NAME"
echo "========================================================"

# Create a simple Scala script to get info about the table and execute it
cat > /tmp/data_info.scala << EOL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Data Information")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
  .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .getOrCreate()

// Read the Hudi table
val df = spark.read.format("hudi").load("$TABLE_PATH")

// Show schema
println("\n=== SCHEMA ===")
df.printSchema()

// Get count
val count = df.count()
println(s"\n=== ROW COUNT: \${count} ===")

// Show sample data (10 rows)
println("\n=== SAMPLE DATA (10 ROWS) ===")
df.show(10, false)

// Exit Spark
spark.stop()
System.exit(0)
EOL

# Get the Spark container ID
SPARK_CONTAINER=$(podman ps | grep spark | awk '{print $1}' | head -n 1)

if [ -z "$SPARK_CONTAINER" ]; then
  # If no Spark container is running, start a new one
  echo "Starting new Spark container to display data info..."
  podman-compose run --rm \
    -v /tmp/data_info.scala:/tmp/data_info.scala \
    --entrypoint "spark-shell --master local[*] -i /tmp/data_info.scala" \
    spark
else
  # If a Spark container is already running, use it
  echo "Using existing Spark container to display data info..."
  podman cp /tmp/data_info.scala $SPARK_CONTAINER:/tmp/data_info.scala
  podman exec -it $SPARK_CONTAINER spark-shell --master local[*] -i /tmp/data_info.scala
fi

echo ""
echo "========================================================"
