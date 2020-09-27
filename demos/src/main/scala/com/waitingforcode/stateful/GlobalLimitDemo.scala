package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.data.configuration.GlobalLimitDataGeneratorConfiguration
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object GlobalLimitDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Global limit")
  import sparkSession.implicits._

  val sourceContext = SourceContext(GlobalLimitDataGeneratorConfiguration.topicName)

  val inputKafkaRecords = sourceContext.inputStream(sparkSession)
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
  ))

  val firstTwoItemsQuery = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .limit(6)

  val checkpointDir = "/tmp/data+ai/stateful/global_limit/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = firstTwoItemsQuery.writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()
}
