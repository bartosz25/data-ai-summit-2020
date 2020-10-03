package com.waitingforcode.data.configuration

object StreamStreamJoinsAdsDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "ads"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq(
      recordJson("2020-05-05T10:26:00", 1),
      recordJson("2020-05-05T10:26:00", 2)
    ),
    Seq(
      // Fall behind the watermark
      recordJson("2020-05-05T10:20:00", 3),
      recordJson("2020-05-05T10:10:00", 4)
    ),
    Seq(
      recordJson("2020-05-05T10:42:00", 5),
      recordJson("2020-05-05T10:56:00", 6)
    ),
    Seq(
      recordJson("2020-05-05T10:57:00", 7),
      recordJson("2020-05-05T10:57:40", 8)
    )
  )

  private def recordJson(eventTime: String, adId: Int) =
    s"""{"event_time": "${eventTime}", "ad_id": ${adId}}"""
}
