package com.waitingforcode.data.configuration

object FlatMapGroupsWithStateDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "flatmapgroupswithstate_demo"

  private val SingleClick = "singleClick"
  private val DoubleClick = "doubleClick"
  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(
      recordJson("2020-05-05T01:22:00", "user1", SingleClick),
      recordJson("2020-05-05T01:26:00", "user1", SingleClick),
    ),
    Seq(
      // This one is a duplicate
      recordJson("2020-05-05T01:31:00", "user2", DoubleClick),
      recordJson("2020-05-05T01:41:00", "user1", DoubleClick)
    ),
    Seq(
      recordJson("2020-05-05T01:42:00", "user1", SingleClick),
      recordJson("2020-05-05T01:56:00", "user2", DoubleClick),
    ),
    Seq(
      recordJson("2020-05-05T01:22:00", "user1", SingleClick),
      recordJson("2020-05-05T01:26:00", "user2", DoubleClick),
    ),
    Seq(
      recordJson("2020-05-05T02:02:00", "user3", DoubleClick),
      recordJson("2020-05-05T02:16:00", "user4", SingleClick),
    )
  )

  private def recordJson(eventTime: String, userLogin: String, clickType: String) =
    s"""{"eventTime": "${eventTime}", "userLogin": "${userLogin}", "clickType": "${clickType}"}"""
}
