package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.source.SparkSessionFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object DropDuplicatesDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Drop duplicates")
  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", s"drop_duplicates_${System.currentTimeMillis()}")
    .option("subscribe", "drop_duplicates_topic")
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

  val checkpointDir = "/tmp/data+ai/stateful/drop_duplicates/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = deduplicatedStream.writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()
}
