package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.OutputDirMultipleStatefulOperations
import com.waitingforcode.data.configuration.MultipleStateOperationsDataGeneratorConfiguration
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object MultipleStateOperationsDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Multiple state operations")
  import sparkSession.implicits._
  val sourceContext = SourceContext(MultipleStateOperationsDataGeneratorConfiguration.topicName)

  val inputKafkaRecords = sourceContext.inputStream(sparkSession)
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
    .sum("value")

  val checkpointDir = "/tmp/data+ai/stateful/multiple_states/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  FileUtils.deleteDirectory(new File(OutputDirMultipleStatefulOperations))
  val consoleWriterQuery = windowStream.writeStream
    .format("json")
    .outputMode(OutputMode.Update)
    .option("checkpointLocation", checkpointDir)
    .foreachBatch(new BatchFilesWriter[Row](OutputDirMultipleStatefulOperations)).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}
