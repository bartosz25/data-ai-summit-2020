package com.waitingforcode.data.configuration

object DropDuplicatesDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "drop_duplicates_demo"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq("""{"event_time": "2020-05-05T01:22:00", "id": 1, "value": 1}"""),
    Seq("""{"event_time": "2020-05-05T03:22:00", "id": 2, "value": 1}"""),
    // Same id (dropDuplicates uses it) but different event_time, won't be integrated
    Seq("""{"event_time": "2020-05-05T01:25:00", "id": 1, "value": 1}"""),
    Seq("""{"event_time": "2020-05-05T01:22:00", "id": 1, "value": 1}"""),
    // New id but too old to be integrated (watermark)
    Seq("""{"event_time": "2020-05-05T01:22:00", "id": 3, "value": 1}""")
  )
}
