package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object DropDuplicatesDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](DropDuplicatesStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecords = testExecutionWrapper.inputStream
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
    StructField("value", IntegerType)
  ))

  val deduplicatedStream = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .dropDuplicates("id", "event_time")

  val writeQuery = testExecutionWrapper.writeToSink(deduplicatedStream)

  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()
}
