package com.waitingforcode.data.configuration

object MultipleStateOperationsDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "multiple_state_operations"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(
      recordJson("2020-05-05T01:22:00", value = 3, id = 1),
      recordJson("2020-05-05T01:26:00", value = 1, id = 2),
    ),
    Seq(
      // This one is a duplicate
      recordJson("2020-05-05T01:31:00", value = 5, id = 1),
      recordJson("2020-05-05T01:41:00", value = 3, id = 3),
      recordJson("2020-05-05T01:26:00", value = 1, id = 4),
    ),
    Seq(
      recordJson("2020-05-05T01:42:00", value = 7, id = 5),
      // This one is a duplicate
      recordJson("2020-05-05T01:36:00", value = 9, id = 2),
    ),
    Seq(
      recordJson("2020-05-05T01:22:00", value = 3, id = 6),
      recordJson("2020-05-05T01:26:00", value = 1, id = 7),
    ),
  )

  private def recordJson(eventTime: String, value: Int, id: Int) =
    s"""{"event_time": "${eventTime}", "id": ${id}, "value": ${value}}"""
}
