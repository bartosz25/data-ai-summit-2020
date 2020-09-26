package com.waitingforcode.source

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def defaultSparkSession(appName: String) = SparkSession.builder()
    .appName(appName).master("local[*]")
    // Set low number of shuffle partitions to investigate the internals easier
    .config("spark.sql.shuffle.partitions", 2)
    .getOrCreate()

}
