package com.waitingforcode.data.configuration

object StreamStreamJoinsClicksDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "clicks"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(
      recordJson("2020-05-05T10:25:10", 1),
      recordJson("2020-05-05T10:26:00", 2)
    ),
    Seq(
      // Fall behind the watermark
      recordJson("2020-05-05T10:20:00", 3),
      recordJson("2020-05-05T10:10:00", 4)
    ),
    Seq(
      // Too late clicks
      recordJson("2020-05-05T10:42:50", 5),
      recordJson("2020-05-05T10:56:50", 6)
    ),
    Seq(
      recordJson("2020-05-05T10:57:00", 7),
      recordJson("2020-05-05T10:57:40", 8)
    )
  )
  private def recordJson(eventTime: String, adId: Int) =
    s"""{"event_time": "${eventTime}", "ad_id": ${adId}}"""

}
