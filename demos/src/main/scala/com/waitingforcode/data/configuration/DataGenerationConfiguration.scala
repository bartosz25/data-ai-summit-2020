package com.waitingforcode.data.configuration

abstract class DataGenerationConfiguration {
  protected var recordsToSend: Iterator[Seq[String]]
  def topicName: String
  def hasNextRecordToSend: Boolean = recordsToSend.hasNext
  def nextRecordToSend: Seq[String] = recordsToSend.next()
}
