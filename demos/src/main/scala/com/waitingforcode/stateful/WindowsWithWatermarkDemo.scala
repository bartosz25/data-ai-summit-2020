package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import com.waitingforcode.stateful.AggregationDemo.sumsStream
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object WindowsWithWatermarkDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](WindowsWithWatermarkStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecords = testExecutionWrapper.inputStream
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("count", IntegerType)
  ))

  val window = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("event_time", "25 minutes")
    .groupBy(functions.window($"event_time", "10 minutes"))

  val writeQuery = testExecutionWrapper.writeToSink(sumsStream, OutputMode.Update)

  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()

}
