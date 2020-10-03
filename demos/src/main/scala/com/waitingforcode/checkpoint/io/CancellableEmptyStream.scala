package com.waitingforcode.checkpoint.io

import java.io.ByteArrayOutputStream

import com.waitingforcode.checkpoint.InMemoryCheckpointStore
import org.apache.hadoop.fs._
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream

class CancellableEmptyStream(checkpointStore: InMemoryCheckpointStore, path: Path)
  extends CancellableFSDataOutputStream(new ByteArrayOutputStream()) {

  private var content = ""

  override def cancel(): Unit = {
    println("Cancelling the output")
    // Since we're buffering things in memory, in case of cancelling, we have nothing to do
    // The default implementation for CancellableFSDataOutputStream (RenameBasedFSDataOutputStream)
    // deletes the created temporary file.
  }

  override def write(bytes: Array[Byte]): Unit = {
    println(s"1 Writing bytes=${new String(bytes)}")
    write(bytes, 0, bytes.length)
  }

  override def write(bytes: Array[Byte], start: Int, length: Int): Unit = {
    println(s"2 Writing bytes=${new String(bytes)}")
    content += new String(bytes)
    content += "\n"
    // super.write(bytes, start, length)
  }

  override def write(i: Int): Unit = {
    println(s"Writing i=${i}")
    super.write(i)
  }

  override def close(): Unit = {
    println("Closing the stream and sending it to in-memory database!")
    println(s"Got ${content}")
    checkpointStore.put(path, content)
  }
}


