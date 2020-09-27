package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.data.configuration.AggregationDataGeneratorConfiguration
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object AggregationDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Aggregation demo")
  import sparkSession.implicits._
  val sourceContext = SourceContext(AggregationDataGeneratorConfiguration.topicName)

  val inputKafkaRecords = sourceContext.inputStream(sparkSession)
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("group_id", IntegerType),
    StructField("value", IntegerType)
  ))

  val sumsStream = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "5 minutes")
    .groupBy(functions.window($"event_time", "10 minutes"))
    .sum("value")

  val checkpointDir = "/tmp/data+ai/stateful/aggregation_demo/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = sumsStream.writeStream
    .format("console")
    .option("truncate", false)
    .outputMode(OutputMode.Complete())
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
