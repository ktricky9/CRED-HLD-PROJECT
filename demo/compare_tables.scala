// Set S3/MinIO configs
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://minio:9000")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin") 
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

// Read original orders table
println("\n===== ORIGINAL TABLE =====")
val ordersDF = spark.read.format("hudi").load("s3a://hudi-data/orders")
ordersDF.createOrReplaceTempView("orders")

// Show sample data with JSON status field
println("Original Table Sample (with nested JSON):")
spark.sql("SELECT order_id, customer_id, status, created_at, __lsn FROM orders LIMIT 10").show(false)

// Read transformed table
println("\n===== TRANSFORMED TABLE =====")
val transformedDF = spark.read.format("hudi").load("s3a://hudi-data/orders_transformed")
transformedDF.createOrReplaceTempView("orders_transformed")

// Show sample data with flattened JSON fields
println("Transformed Table Sample (with flattened fields):")
spark.sql("""
  SELECT 
    order_id, 
    customer_id,
    status_code, 
    status_message, 
    status_severity 
  FROM orders_transformed 
  LIMIT 10
""").show(false)

// Compare order counts in both tables
println("\n===== TABLE STATISTICS =====")
println("Original table count: " + ordersDF.count())
println("Transformed table count: " + transformedDF.count())

// Show schema difference
println("\n===== SCHEMA COMPARISON =====")
println("Original table schema:")
ordersDF.printSchema()

println("\nTransformed table schema:")
transformedDF.printSchema()

// Exit the Spark shell
System.exit(0)
