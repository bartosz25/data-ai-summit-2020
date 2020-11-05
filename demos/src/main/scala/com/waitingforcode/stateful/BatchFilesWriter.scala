package com.waitingforcode.stateful

import org.apache.spark.sql.{Dataset, SaveMode}

class BatchFilesWriter[T](inputPath: String) extends ((Dataset[T], Long) => Unit) {
  override def apply(datasetToWrite: Dataset[T], batchNumber: Long): Unit = {
    datasetToWrite.write.mode(SaveMode.Overwrite).json(s"${inputPath}/${batchNumber}")
    println(s"Wrote ${batchNumber} batch")
  }
}