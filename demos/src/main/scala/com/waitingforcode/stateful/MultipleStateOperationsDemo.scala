package com.waitingforcode.stateful

import com.waitingforcode.TestExecutionWrapper
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, functions}

object MultipleStateOperationsDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[Row](MultipleStateOperationsStatefulAppConfig)
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
    .dropDuplicates("id")

  val windowStream = deduplicatedStream
    .groupBy(functions.window($"event_time", "10 minutes"))
    .sum("value").select($"window", $"sum(value)".as("sum"))

  val writeQuery = testExecutionWrapper.writeToSink(windowStream, OutputMode.Update)

  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()

}
