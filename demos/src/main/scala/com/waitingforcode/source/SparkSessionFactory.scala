package com.waitingforcode.source

import com.waitingforcode.stateful.StatefulAppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object SparkSessionFactory {

  def defaultSparkSession(appConfig: StatefulAppConfig) = SparkSession.builder()
    .appName(appConfig.appName).master("local[*]")
    // Set low number of shuffle partitions to investigate the internals easier
    .config("spark.sql.shuffle.partitions", 2)
    .config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, "com.waitingforcode.statestore.MapDBStateStoreProvider")
    .config(SQLConf.MIN_BATCHES_TO_RETAIN.key, 3)
    .config("spark.sql.streaming.stateStore.mapdb.checkpointPath", appConfig.mapDbCheckpointPath)
    .config("spark.sql.streaming.stateStore.mapdb.localPath", appConfig.mapDbLocalPath)
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
