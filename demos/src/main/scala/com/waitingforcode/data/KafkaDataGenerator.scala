package com.waitingforcode.data

import com.waitingforcode.data.configuration.{AggregationDataGeneratorConfiguration, CustomCheckpointDataGeneratorConfiguration, DropDuplicatesDataGeneratorConfiguration, FlatMapGroupsWithStateDataGeneratorConfiguration, GlobalLimitDataGeneratorConfiguration, MapGroupsWithStateDataGeneratorConfiguration, MultipleStateOperationsDataGeneratorConfiguration, StreamStreamJoinsAdsDataGeneratorConfiguration, StreamStreamJoinsClicksDataGeneratorConfiguration}
import com.waitingforcode.source.KafkaConfiguration
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.sys.process._

object KafkaDataGenerator extends App {

  val configurations = Map(
    "aggregation" -> AggregationDataGeneratorConfiguration,
    "drop_duplicates" -> DropDuplicatesDataGeneratorConfiguration,
    "global_limit" -> GlobalLimitDataGeneratorConfiguration,
    "multiple_states" -> MultipleStateOperationsDataGeneratorConfiguration,
    "mapgroupswithstate" -> MapGroupsWithStateDataGeneratorConfiguration,
    "flatmapgroupswithstate" -> FlatMapGroupsWithStateDataGeneratorConfiguration,
    "streamstreamjoinads" -> StreamStreamJoinsAdsDataGeneratorConfiguration,
    "streamstreamjoinclicks" -> StreamStreamJoinsClicksDataGeneratorConfiguration,
    "customcheckpoint" -> CustomCheckpointDataGeneratorConfiguration
  )

  val configuration = configurations(args(0))

  println("== Deleting already existing topic ==")
  val deleteTopicResult =
    Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${configuration.topicName} --delete").run()
  deleteTopicResult.exitValue()
  println(s"== Creating ${configuration.topicName} ==")
  val createTopicResult =
    Process(s"docker exec broker_kafka_1 kafka-topics.sh --bootstrap-server localhost:29092 --topic ${configuration.topicName} --create --partitions 2").run()
  createTopicResult.exitValue()

  val kafkaProducer = new KafkaProducer[String, String](Map[String, Object](
    "bootstrap.servers" -> KafkaConfiguration.BootstrapServer,
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  ).asJava)
  println("Press enter to send the first message")
  scala.io.StdIn.readLine()
  while (configuration.hasNextRecordToSend) {
    val recordsToSend = configuration.nextRecordToSend
    recordsToSend.foreach(recordToSend => {
      println(s"Sending ${recordToSend}")
      kafkaProducer.send(new ProducerRecord[String, String](configuration.topicName, recordToSend))
    })
    kafkaProducer.flush()
    println("Press enter to send another message")
    scala.io.StdIn.readLine()
  }

  println("All messages were send")

}


