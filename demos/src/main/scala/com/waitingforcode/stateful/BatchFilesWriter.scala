package com.waitingforcode.stateful

import org.apache.spark.sql.Dataset

class BatchFilesWriter[T](inputPath: String) extends ((Dataset[T], Long) => Unit) {
  override def apply(datasetToWrite: Dataset[T], batchNumber: Long): Unit = {
    datasetToWrite.write.json(s"${inputPath}/${batchNumber}")
    println(s"Wrote ${batchNumber} batch")
  }
}