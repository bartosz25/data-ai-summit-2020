package com.waitingforcode

import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import com.waitingforcode.stateful.{AggregationStatefulAppConfig, BatchFilesWriter, StatefulAppConfig}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{Dataset, SparkSession}

package object stateful {

  def explainQueryPlan(streamingQuery: StreamingQuery): Unit = {
    new Thread(
      new Runnable() {
        override def run(): Unit = {
          while (streamingQuery.status == null || streamingQuery.lastProgress == null) {
            Thread.sleep(1000L)
          }
          streamingQuery.explain(true)
        }
      }
    ).start()
  }

  def getSparkSessionAndSourceContext(appConfig: StatefulAppConfig): (SparkSession, SourceContext) = {
    AggregationStatefulAppConfig.cleanUpDirs
    val sparkSession = SparkSessionFactory.defaultSparkSession(appConfig)
    val sourceContext = SourceContext(appConfig.dataConfig.topicName)
    (sparkSession, sourceContext)
  }
}

class TestExecutionWrapper[T](appConfig: StatefulAppConfig, cleanUp: Boolean = true) {

  if (cleanUp) AggregationStatefulAppConfig.cleanUpDirs

  val sparkSession = SparkSessionFactory.defaultSparkSession(appConfig)
  val sourceContext = SourceContext(appConfig.dataConfig.topicName)

  val inputStream = sourceContext.inputStream(sparkSession)

  def writeToSink(dataset: Dataset[T], outputMode: OutputMode = OutputMode.Append): StreamingQuery = {
    dataset.writeStream
      .outputMode(outputMode)
      .foreachBatch(new BatchFilesWriter[T](appConfig.outputDir))
      .option("checkpointLocation", appConfig.checkpointDir).start()
  }

}
