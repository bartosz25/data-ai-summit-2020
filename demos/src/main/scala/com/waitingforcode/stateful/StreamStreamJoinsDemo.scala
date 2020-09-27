package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object StreamStreamJoinsDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Stream-stream join demo")
  import sparkSession.implicits._
  val sourceContextClicks = SourceContext("stream_stream_clicks")
  val sourceContextAds = SourceContext("stream_stream_ads")

  val inputKafkaRecordsClicks = sourceContextClicks.inputStream(sparkSession)
  val inputKafkaRecordSchemaClicks = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
    StructField("value", IntegerType)
  ))
  val clicksStream = inputKafkaRecordsClicks.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaClicks).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "2 seconds")

  val inputKafkaRecordsAds = sourceContextAds.inputStream(sparkSession)
  val inputKafkaRecordSchemaAds = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
    StructField("value", IntegerType)
  ))
  val adsStream = inputKafkaRecordsAds.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaAds).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "20 seconds")

  val joinedStream = adsStream.join(clicksStream, adsStream("ad_id") === clicksStream("ad_id"), "leftOuter")

  val checkpointDir = "/tmp/data+ai/stateful/stream_stream_joins/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = joinedStream.writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
