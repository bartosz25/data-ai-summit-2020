package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object AggregationDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](AggregationStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecords = testExecutionWrapper.inputStream
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

  val writeQuery = testExecutionWrapper.writeToSink(sumsStream)

  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()

}
