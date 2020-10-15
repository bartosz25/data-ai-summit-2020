package com.waitingforcode.blogposts

import org.apache.spark.sql.catalyst.optimizer.PushDownPredicates
import org.apache.spark.sql.{SparkSession, functions}

object JoinPreJoinFilterExample extends App {

  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Structured Streaming inner join test")
    // Exclude this rule to see the preFilter in action
    // Otherwise, it's pushed down to the data source
    .config("spark.sql.optimizer.excludedRules", s"${PushDownPredicates.ruleName}")
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

  val joinCondition =
    """
      |mainValue = joinedValue AND joinedValue + 3 > 2
      |AND mainValue > 1 AND mainValue + joinedValue > 2
      |""".stripMargin
  val stream = mainEventsStream.join(joinedEventsStream,
    functions.expr(joinCondition), "inner")


  val query = stream.writeStream.format("console").option("truncate", false).start()

  query.awaitTermination()

}
