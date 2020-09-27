package com.waitingforcode.data.configuration

object GlobalLimitDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "global_limit_demo"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(jsonRecord("2020-05-05T14:22:00", 1),
      jsonRecord("2020-05-05T15:22:00", 2),
      jsonRecord("2020-05-05T15:22:30", 3)),
    Seq(jsonRecord("2020-05-05T14:22:00", 4),
      jsonRecord("2020-05-05T15:22:00", 5),
      jsonRecord("2020-05-05T15:22:30", 6),
      jsonRecord("2020-05-05T15:26:30", 7),
      jsonRecord("2020-05-05T15:42:30", 8)),
    Seq(jsonRecord("2020-05-05T16:22:00", 9),
      jsonRecord("2020-05-05T16:22:00", 10),
      jsonRecord("2020-05-05T16:22:30", 11),
      jsonRecord("2020-05-05T16:26:30", 12)),
    Seq(jsonRecord("2020-05-05T16:42:00", 13),
      jsonRecord("2020-05-05T16:45:00", 14),
      jsonRecord("2020-05-05T16:48:30", 15))
  )

  def jsonRecord(eventTime: String, id: Int) = {
    s"""{ "event_time": "${eventTime}", "id": ${id} }"""
  }
}
