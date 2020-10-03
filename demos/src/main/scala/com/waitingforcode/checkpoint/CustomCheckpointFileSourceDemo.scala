package com.waitingforcode.checkpoint

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

/**
 * The goal of this demo is to show that the custom checkpoint manager applies
 * not only for the metadata checkpointing but also for the metadata in file source/sink!
 */
object CustomCheckpointFileSourceDemo extends App {

  val streamDir = "/tmp/data+ai/custom_checkpoint_manager/"
  val dataDir = s"${streamDir}/data-input"
  FileUtils.deleteDirectory(new File(streamDir))
  new Thread(new Runnable {
    override def run(): Unit = {
      println("Starting files generation")
      var nr = 1
      while (nr <= 4000) {
        val fileName = s"${dataDir}/${nr}.txt"
        val jsonContent = nr.toString
        FileUtils.writeStringToFile(new File(fileName), jsonContent)
        nr += 1
        Thread.sleep(5000L)
      }
    }
  }).start()

  val sparkSession = SparkSession.builder()
    .appName("Spark 3.0: Custom checkpoint demo").master("local[*]")
    // TODO: how to use this class in PySpark? ==> add a custom JAR
    .config("spark.sql.streaming.checkpointFileManagerClass", classOf[InMemoryCheckpointManager].getCanonicalName)
    .config("spark.sql.streaming.minBatchesToRetain", 3)
    // Disabling compaction is not possible, the source and sink expect a positive
    // value. You can set it to a very big number like `Int.MaxValue` but you will see that sooner or later
    // your streaming application will fail and it's supposed to be long-running!
    // So, let's keep it small and see the failure by ourselves
    // The failure is the proof that implementing a not file-based checkpoint manager
    // is a bit hacky.
    .config("spark.sql.streaming.fileSink.log.compactInterval", 2)
    .config("spark.sql.streaming.fileSource.log.compactInterval", 2)
    .getOrCreate()
  sparkSession.sqlContext.setConf("table_name", "CheckpointStoreFileSource")

  val writeQuery = sparkSession.readStream.text(dataDir)
    .writeStream
    .format("json")
    .option("path", s"${streamDir}/data-output")
    // the checkpoint location will be created but will remain empty as the CheckpointManager
    // doesn't interact with it
    .option("checkpointLocation", s"${streamDir}/checkpoint")


  writeQuery.start().awaitTermination()

}
