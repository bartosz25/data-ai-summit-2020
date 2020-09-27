package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.source.SparkSessionFactory
import com.waitingforcode.stateful.AggregationDemo.sumsStream
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object WindowsWithWatermarkDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Window demo")
  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", s"window_demo_${System.currentTimeMillis()}")
    .option("subscribe", "window_demo_topic")
    .option("startingOffsets", "EARLIEST")
    .load()

  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("count", IntegerType)
  ))

  val window = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .groupBy(functions.window($"event_time", "10 minutes"))

  val checkpointDir = "/tmp/data+ai/stateful/window_demo/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = sumsStream.writeStream
    .format("console")
    .option("truncate", false)
    .outputMode(OutputMode.Update)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
