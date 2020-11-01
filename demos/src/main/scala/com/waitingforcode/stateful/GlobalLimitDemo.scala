package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object GlobalLimitDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](GlobalLimitStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecords = testExecutionWrapper.inputStream
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
  ))

  val firstTwoItemsQuery = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .limit(2)

  val writerQuery = testExecutionWrapper.writeToSink(firstTwoItemsQuery)

  explainQueryPlan(writerQuery)

  writerQuery.awaitTermination()
}

