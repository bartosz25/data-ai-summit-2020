package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.OutputDirDropDuplicates
import com.waitingforcode.data.configuration.DropDuplicatesDataGeneratorConfiguration
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, functions}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object DropDuplicatesDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Drop duplicates")
  import sparkSession.implicits._
  val sourceContext = SourceContext(DropDuplicatesDataGeneratorConfiguration.topicName)

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
    .dropDuplicates("id", "event_time")

  val checkpointDir = "/tmp/data+ai/stateful/drop_duplicates/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  FileUtils.deleteDirectory(new File(OutputDirDropDuplicates))
  val consoleWriterQuery = deduplicatedStream.writeStream
    .foreachBatch(new BatchFilesWriter[Row](OutputDirDropDuplicates))
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()
}
