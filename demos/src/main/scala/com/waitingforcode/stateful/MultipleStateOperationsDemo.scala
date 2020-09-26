package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.source.SparkSessionFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object MultipleStateOperationsDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Multiple state operations")
  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", s"multiple_states_${System.currentTimeMillis()}")
    .option("subscribe", "multiple_states_topic")
    .option("startingOffsets", "EARLIEST")
    .load()
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
    StructField("value", IntegerType)
  ))

  val deduplicatedStream = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .dropDuplicates("id")

  val windowStream = deduplicatedStream
    .groupBy(functions.window($"event_time", "10 minutes"))
    .sum("value")

  val checkpointDir = "/tmp/data+ai/stateful/multiple_states/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = windowStream.writeStream
    .format("console")
    .option("truncate", false)
    .outputMode(OutputMode.Update)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
