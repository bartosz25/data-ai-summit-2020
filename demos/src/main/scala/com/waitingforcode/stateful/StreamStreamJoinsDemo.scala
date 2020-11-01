package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import com.waitingforcode.data.configuration.StreamStreamJoinsClicksDataGeneratorConfiguration
import com.waitingforcode.source.SourceContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object StreamStreamJoinsDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](StreamToStreamJoinStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecordsAds = testExecutionWrapper.inputStream
  val inputKafkaRecordSchemaAds = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("ad_id", IntegerType)
  ))
  val adsStream = inputKafkaRecordsAds.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaAds).as("record"))
    .selectExpr("record.*")

  val sourceContextClicks = SourceContext(StreamStreamJoinsClicksDataGeneratorConfiguration.topicName)
  val inputKafkaRecordsClicks = sourceContextClicks.inputStream(testExecutionWrapper.sparkSession)
  val inputKafkaRecordSchemaClicks = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("ad_id", IntegerType)
  ))
  val clicksStream = inputKafkaRecordsClicks.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchemaClicks).as("record"))
    .selectExpr("record.*")
    .select($"event_time".as("click_time"), $"ad_id".as("clicks_ad_id"))
    .withWatermark("click_time", "20 seconds")


  val joinedStream = adsStream.join(clicksStream,
    adsStream("ad_id") === clicksStream("clicks_ad_id") &&
      functions.expr("event_time >= click_time"), "leftOuter")

  val writeQuery = testExecutionWrapper.writeToSink(joinedStream)
// TODO: it's failing, use the storeName from StateStoreProvider StateStoreId parmaeter!
  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()

}
