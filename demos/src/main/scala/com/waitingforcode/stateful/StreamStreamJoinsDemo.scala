package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.OutputDirStreamStreamJoins
import com.waitingforcode.data.configuration.{StreamStreamJoinsAdsDataGeneratorConfiguration, StreamStreamJoinsClicksDataGeneratorConfiguration}
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object StreamStreamJoinsDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Stream-stream join demo")
  import sparkSession.implicits._
  val sourceContextClicks = SourceContext(StreamStreamJoinsClicksDataGeneratorConfiguration.topicName)
  val sourceContextAds = SourceContext(StreamStreamJoinsAdsDataGeneratorConfiguration.topicName)

  val inputKafkaRecordsClicks = sourceContextClicks.inputStream(sparkSession)
  val inputKafkaRecordSchemaClicks = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("ad_id", IntegerType)
  ))
  val clicksStream = inputKafkaRecordsClicks.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaClicks).as("record"))
    .selectExpr("record.*")
    .select($"event_time".as("click_time"), $"ad_id".as("clicks_ad_id"))
    .withWatermark("click_time", "20 seconds")

  val inputKafkaRecordsAds = sourceContextAds.inputStream(sparkSession)
  val inputKafkaRecordSchemaAds = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("ad_id", IntegerType)
  ))
  val adsStream = inputKafkaRecordsAds.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaAds).as("record"))
    .selectExpr("record.*")

  val joinedStream = adsStream.join(clicksStream,
    adsStream("ad_id") === clicksStream("clicks_ad_id") &&
      functions.expr("event_time >= click_time"), "leftOuter")

  val checkpointDir = "/tmp/data+ai/stateful/stream_stream_joins/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  FileUtils.deleteDirectory(new File(OutputDirStreamStreamJoins))
  val consoleWriterQuery = joinedStream.writeStream
    .option("checkpointLocation", checkpointDir)
    .foreachBatch(new BatchFilesWriter[Row](OutputDirStreamStreamJoins)).start()
// TODO: it's failing, use the storeName from StateStoreProvider StateStoreId parmaeter!
  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
