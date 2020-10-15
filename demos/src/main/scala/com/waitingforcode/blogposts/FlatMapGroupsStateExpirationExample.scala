package com.waitingforcode.blogposts

import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.GroupStateTimeout.ProcessingTimeTimeout
import org.apache.spark.sql.{Row, SparkSession, functions}

object FlatMapGroupsStateExpirationExample extends App {

  val session = SparkSession.builder()
    .appName("State expiration").master("local[*]")
    // Keep very small number of shuffle
    // partitions to see much easier what happens
    .config("spark.sql.shuffle.partitions", 1)
    .getOrCreate()
  import session.implicits._

  val rateStreamLeft = session.readStream
    .format("rate")
    .option("rowsPerSecond", 1)
    .option("numPartitions", 1)
    .load()
    .withColumn("mapKey", functions.lit(0))
    .filter("value = 1")


  def removeCreatedStatesMapper(key: Long, values: Iterator[Row], state: GroupState[Int]): String = {
    if (state.hasTimedOut) {
      println("Removing the state")
      LocalSynchronizer.wasRemoved = true
      state.remove()
    } else if (!LocalSynchronizer.wasRemoved && !state.exists) {
      println("No state, the function called for the first time, set the state")
      state.setTimeoutDuration("1 seconds")
      state.update(1)
    }
    // return a dummy value, we don't care about it right now
    "abc"
  }

  val query = rateStreamLeft
    .groupByKey(row => row.getAs[Long]("value"))
    .mapGroupsWithState(ProcessingTimeTimeout)(removeCreatedStatesMapper _)
    .writeStream
    .format("console")
    .outputMode("update")
    //.option("checkpointLocation", "/tmp/data+ai/blog_posts/state_stores_example")
    .option("truncate", false)
    .start()

  query.awaitTermination()

}

object LocalSynchronizer {
  var wasRemoved = false
}