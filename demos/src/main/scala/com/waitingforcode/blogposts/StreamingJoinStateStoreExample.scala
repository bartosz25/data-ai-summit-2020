package com.waitingforcode.blogposts

import org.apache.spark.sql.SparkSession

object StreamingJoinStateStoreExample extends App {

  val session = SparkSession.builder()
    .appName("Join to join streaming example").master("local[*]")
    // Join involves shuffle, so keep very small number of shuffle
    // partitions to see much easier what happens
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()

  val rateStreamLeft = session.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
  val rateStreamRight = session.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()

  import org.apache.spark.sql.functions._

  val fooStream = rateStreamLeft
    .select(col("value").as("fooId"), col("timestamp").as("fooTime"))

  val barStream = rateStreamRight
    .select(col("value").as("barId"), col("timestamp").as("barTime"))

  val query = fooStream
    .withWatermark("fooTime", "5 seconds")
    .join(
      barStream.withWatermark("barTime", "5 seconds"),
      expr("barId = fooId"), joinType = "inner"
    )
    .writeStream
    .format("console")
    .option("checkpointLocation", "/tmp/data+ai/blog_posts/state_stores_example")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}
