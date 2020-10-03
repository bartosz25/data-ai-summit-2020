package com.waitingforcode.committer

import org.apache.spark.sql.SparkSession

object CustomCommitProtocolDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Custom Commit Protocol").master("local[*]").getOrCreate()




}
