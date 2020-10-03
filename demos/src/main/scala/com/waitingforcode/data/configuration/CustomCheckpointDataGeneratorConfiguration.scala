package com.waitingforcode.data.configuration

object CustomCheckpointDataGeneratorConfiguration extends DataGenerationConfiguration {

  override def topicName: String = "custom_checkpoint"

  override protected var recordsToSend: Iterator[Seq[String]] = Iterator(
    Seq("""{"event_time": "2020-05-05T01:22:00", "id": 1}"""),
    Seq("""{"event_time": "2020-05-05T03:22:00", "id": 2}"""),
    Seq("""{"event_time": "2020-05-05T01:25:00", "id": 3}"""),
    Seq("""{"event_time": "2020-05-05T01:22:00", "id": 4}""")
  )
}
