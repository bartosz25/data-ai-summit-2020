package com.waitingforcode.data.configuration

object WindowsWithWatermarkDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "windows_watermark_demo"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(
      recordJson("2020-05-05T01:22:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:26:00", value = 1, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:31:00", value = 5, groupId = 1),
      recordJson("2020-05-05T01:41:00", value = 3, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:42:00", value = 7, groupId = 1),
      recordJson("2020-05-05T01:36:00", value = 9, groupId = 1),
    ),
    Seq(
      recordJson("2020-05-05T01:22:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:26:00", value = 1, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:23:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:25:00", value = 1, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:32:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:36:00", value = 1, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:42:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:46:00", value = 1, groupId = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:52:00", value = 3, groupId = 1),
      recordJson("2020-05-05T01:56:00", value = 1, groupId = 2),
    )
  )

  private def recordJson(eventTime: String, value: Int, groupId: Int = 1) =
    s"""{"event_time": "${eventTime}", "group_id": ${groupId}, "value": ${value}}"""
}
