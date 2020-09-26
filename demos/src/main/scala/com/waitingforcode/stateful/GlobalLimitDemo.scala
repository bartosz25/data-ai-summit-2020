package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.source.SparkSessionFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object GlobalLimitDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] Global limit")
  import sparkSession.implicits._

  val inputKafkaRecords = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("client.id", s"global_limit_${System.currentTimeMillis()}")
    .option("subscribe", "global_limit_topic")
    .option("startingOffsets", "EARLIEST")
    .load()
  val inputKafkaRecordSchema = StructType(Array(
    StructField("event_time", TimestampType),
    StructField("id", IntegerType),
    StructField("value", IntegerType)
  ))

  val firstTwoItemsQuery = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .limit(2)

  val checkpointDir = "/tmp/data+ai/stateful/global_limit/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = firstTwoItemsQuery.writeStream
    .format("console")
    .option("truncate", false)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()
}
