package com.waitingforcode.stateful

import java.io.File
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.waitingforcode.{OutputDirMapGroupsWithState, TestExecutionWrapper}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.collection.mutable

object MapGroupsWithStateDemo extends App {

  val testExecutionWrapper = new TestExecutionWrapper[UserClicks](MapGroupsWithStateStatefulAppConfig)
  import testExecutionWrapper.sparkSession.implicits._

  val inputKafkaRecords = testExecutionWrapper.inputStream
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
    .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(ClickActionMapper.mapUserActionsWithState)

  val sessionsToOutput = usersWithSessions.filter(session => session.isDefined)
    .map(sessionToWrite => sessionToWrite.get)

  val checkpointDir = "/tmp/data+ai/stateful/mapgroupswithstate_demo/checkpoint"
  FileUtils.deleteDirectory(new File(checkpointDir))
  FileUtils.deleteDirectory(new File(OutputDirMapGroupsWithState))
  val writeQuery = testExecutionWrapper.writeToSink(sessionsToOutput, OutputMode.Update)

  explainQueryPlan(writeQuery)

  writeQuery.awaitTermination()

}

case class ClickAction(eventTime: Timestamp, userLogin: String, clickType: String)
case class UserClicks(userLogin: String, lastClick: Timestamp, clicksByType: Map[String, Int])
case class UserClicksState(userLogin: String, lastClick: Timestamp, clicksByType: Map[String, Int]) {

  def toUserClicks = UserClicks(userLogin, lastClick, clicksByType)

  def handleClicks(values: Iterator[ClickAction]): UserClicksState = {
    if (!values.hasNext) {
      this
    } else {
      var newLastClickTime = lastClick
      val newClicksByType = mutable.Map[String, Int](clicksByType.toSeq: _*)
      values.foreach(click => {
        if (click.eventTime.after(newLastClickTime)) {
          newLastClickTime = click.eventTime
        }
        newClicksByType.put(click.clickType, newClicksByType.getOrElse(click.clickType, 0) + 1)
      })
      this.copy(lastClick = newLastClickTime, clicksByType = newClicksByType.toMap)
    }
  }
}
object UserClicksState {
  def fromClickAction(clickAction: ClickAction): UserClicksState = {
    UserClicksState(clickAction.userLogin, clickAction.eventTime, Map(clickAction.clickType -> 1))
  }
}
object ClickActionMapper {

  private val StateTimeToLive = TimeUnit.MINUTES.toMillis(10)

  def mapUserActionsWithState(key: String, values: Iterator[ClickAction],
                    state: GroupState[UserClicksState]): Option[UserClicks] = {
    if (state.hasTimedOut) {
      val expiredState = state.get
      val userClicks = expiredState.toUserClicks
      state.remove()
      Some(userClicks)
    } else {
      val stateToChange = state.getOption.getOrElse(UserClicksState.fromClickAction(values.next()))
      val stateAfterClicks = stateToChange.handleClicks(values)
      state.update(stateAfterClicks)
      state.setTimeoutTimestamp(state.getCurrentWatermarkMs() + StateTimeToLive)
      None
    }
  }
}