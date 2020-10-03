package com.waitingforcode.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object SparkSessionFactory {

  def defaultSparkSession(appName: String) = SparkSession.builder()
    .appName(appName).master("local[*]")
    // Set low number of shuffle partitions to investigate the internals easier
    .config("spark.sql.shuffle.partitions", 2)
    .config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, "com.waitingforcode.statestore.MapDBStateStoreProvider")
    .config("spark.sql.streaming.stateStore.mapdb.checkpointPath", "/tmp/data+ai/mapdb/checkpoint")
    .config("spark.sql.streaming.stateStore.mapdb.localPath", "/tmp/data+ai/mapdb/local")
    .getOrCreate()

}

case class SourceContext(topicName: String) {
  def inputStream(sparkSession: SparkSession) = {
    sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", topicName)
      .option("startingOffsets", "EARLIEST")
      .load()
  }
}
