package com.waitingforcode.checkpoint

import com.waitingforcode.checkpoint.CustomCheckpointFileSourceDemo.streamDir
import com.waitingforcode.data.configuration.CustomCheckpointDataGeneratorConfiguration
import com.waitingforcode.source.SourceContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{SparkSession, functions}

object CustomCheckpointDemo extends App {

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: custom checkpoint reader demo size").master("local[*]")
    .config("spark.sql.streaming.checkpointFileManagerClass", classOf[InMemoryCheckpointManager].getCanonicalName)
    .config("spark.sql.streaming.minBatchesToRetain", 2)
    .getOrCreate()
  sparkSession.sqlContext.setConf("table_name", "CustomCheckpointStore")
  import sparkSession.implicits._
  val sourceContext = SourceContext(CustomCheckpointDataGeneratorConfiguration.topicName)

  val inputKafkaRecords = sourceContext.inputStream(sparkSession)
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType)
  ))

  val inputStream = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")

  val writeQuery = inputStream
    .writeStream.format("console").option("truncate", "false")
    // the checkpoint location will be created but will remain empty as the CheckpointManager
    // doesn't interact with it
    .option("checkpointLocation", s"${streamDir}/checkpoint")

  writeQuery.start().awaitTermination()

}
