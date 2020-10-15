package com.waitingforcode.blogposts

import org.apache.spark.sql.{SparkSession, functions}

object JoinValueWatermarkPredicateExample extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join test")
    .config("spark.sql.shuffle.partitions", "2")
    .master("local[2]").getOrCreate()

  import sparkSession.implicits._

  val mainEventsStream = sparkSession.readStream.format("rate").option("rowsPerSecond", "3")
    .option("numPartitions", "2").load()
    .select($"value".as("mainValue"), $"timestamp".as("mainTimestamp"))
    .withWatermark("mainTimestamp", "10 seconds")
  val joinedEventsStream = sparkSession.readStream.format("rate").option("rowsPerSecond", "3")
    .option("numPartitions", "2").load()
    .select($"value".as("joinedValue"), $"timestamp".as("joinedTimestamp"))
    .withWatermark("joinedTimestamp", "20 seconds")


  val keyPredicateExpression = "mainTimestamp = joinedTimestamp"
  val valuePredicateExpression = "mainValue = joinedValue AND mainTimestamp >=  joinedTimestamp + interval 2 seconds"

  val stream = mainEventsStream.join(joinedEventsStream,
    functions.expr(keyPredicateExpression), "left_outer")


  val query = stream.writeStream.format("console").option("truncate", false).start()

  query.awaitTermination()

}
