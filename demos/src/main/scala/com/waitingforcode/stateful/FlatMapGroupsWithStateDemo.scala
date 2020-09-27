package com.waitingforcode.stateful

import java.io.File
import java.util.concurrent.TimeUnit

import com.waitingforcode.data.configuration.FlatMapGroupsWithStateDataGeneratorConfiguration
import com.waitingforcode.source.{SourceContext, SparkSessionFactory}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object FlatMapGroupsWithStateDemo extends App {

  val sparkSession = SparkSessionFactory.defaultSparkSession("[stateful] flatMapGroupsWithState demo")
  import sparkSession.implicits._
  val sourceContext = SourceContext(FlatMapGroupsWithStateDataGeneratorConfiguration.topicName)

  val inputKafkaRecords = sourceContext.inputStream(sparkSession)

  val inputKafkaRecordSchema = StructType(Array(
    StructField("eventTime", TimestampType),
    StructField("userLogin", StringType),
    StructField("clickType", StringType)
  ))

  val userClickGroups = inputKafkaRecords.selectExpr("CAST(value AS STRING)")
    .select(functions.from_json($"value", inputKafkaRecordSchema).as("record"))
    .selectExpr("record.*")
    .withWatermark("eventTime", "10 minutes")
    .as[ClickAction]
    .groupByKey(click => click.userLogin)

  val usersWithSessions = userClickGroups
    .flatMapGroupsWithState(OutputMode.Update(),
      GroupStateTimeout.EventTimeTimeout())(ClickActionFlatMapper.flatMapUserActionsWithState)

  val checkpointDir = "/tmp/data+ai/stateful/flatmapgroupswithstate/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  val consoleWriterQuery = usersWithSessions.writeStream
    .format("console")
    .option("truncate", false)
    .outputMode(OutputMode.Update)
    .option("checkpointLocation", checkpointDir).start()

  explainQueryPlan(consoleWriterQuery)

  consoleWriterQuery.awaitTermination()

}

object ClickActionFlatMapper {

  private val StateTimeToLive = TimeUnit.MINUTES.toMillis(10)

  // Let's keep it simple, this method is slightly similar to the
  // ClickActionMapper#mapUserActionsWithState except it returns an iterator
  def flatMapUserActionsWithState(key: String, values: Iterator[ClickAction],
                              state: GroupState[UserClicksState]): Iterator[UserClicks] = {
    if (state.hasTimedOut) {
      val expiredState = state.get
      val userClicks = expiredState.toUserClicks
      state.remove()
      Iterator(userClicks)
    } else {
      val stateToChange = state.getOption.getOrElse(UserClicksState.fromClickAction(values.next()))
      val stateAfterClicks = stateToChange.handleClicks(values)
      state.update(stateAfterClicks)
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + StateTimeToLive)
      Iterator.empty
    }
  }
}