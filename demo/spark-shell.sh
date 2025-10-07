#!/bin/bash
# Launch Spark shell with MinIO access in a container

echo "Launching Spark shell with MinIO access..."
echo "This will allow you to interactively query your Hudi tables"
echo "Example usage once in the shell:"
echo ""
echo "scala> val df = spark.read.format(\"hudi\").load(\"s3a://hudi-data/orders\")"
echo "scala> df.show(5)"
echo ""

podman-compose run \
  -e PYTHONUNBUFFERED=1 \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin \
  -e SPARK_LOCAL_IP=0.0.0.0 \
  -p 4040:4040 \
  --entrypoint "spark-shell --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.1" \
  spark
