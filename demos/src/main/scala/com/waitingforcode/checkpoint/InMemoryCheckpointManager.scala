package com.waitingforcode.checkpoint

import com.waitingforcode.checkpoint.io.{CancellableEmptyStream, MemorySeekableEmptyInputStream, MemorySeekableInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, Path, PathFilter}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.CancellableFSDataOutputStream

import scala.collection.mutable


class InMemoryCheckpointManager(path: Path, hadoopConf: Configuration) extends CheckpointFileManager {

  private val inMemoryTable = new InMemoryCheckpointStore(hadoopConf.get("table_name"))

  override def createAtomic(path: Path, overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    new CancellableEmptyStream(inMemoryTable, path)
  }

  override def open(path: Path): FSDataInputStream = {
    println(s"Opening ${path}")
    val text = "test data"
    // TODO: question for me ==> Why this InputStream has to be Seekable or Positionedreadable?
    // In is not an instance of Seekable or PositionedReadable <== error returned if it's not the case
    // as below:
    //     new FSDataInputStream(
    //        new java.io.ByteArrayInputStream(text.getBytes(java.nio.charset.StandardCharsets.UTF_8.name)
    //        )
    //    )
    // We use an external data store, no risk of small files problem
    val inputStream = if (path.getName.endsWith(".compact")) {
      new MemorySeekableEmptyInputStream()
    } else {
      new MemorySeekableInputStream(inMemoryTable.getExistingEntry(path).getBytes("UTF-8"))
    }

    new FSDataInputStream(inputStream)
  }

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    println(s"Listing files for ${path}")
    inMemoryTable.allKeys.filter(keyPath => filter.accept(keyPath))
      .map(matchingPath => {
        val status = new FileStatus()
        status.setPath(matchingPath)
        status
      })
      .toArray
  }

  override def mkdirs(path: Path): Unit = {
    println(s"Making dirs for ${path}")
  }

  override def exists(path: Path): Boolean = {
    println(s"Checking if $path exists?")
    if (path.getName.endsWith(".compact")) {
      false
    } else {
      inMemoryTable.get(path).nonEmpty
    }
  }

  override def delete(path: Path): Unit = {
    println(s"Deleting path=${path}")
    inMemoryTable.remove(path)
  }

  override def isLocal: Boolean = {
    println("Checking if is local?")
    // TODO: when it's used? There is an if-else conditional with this?
    true
  }
}

class InMemoryCheckpointStore(tableName: String) {
  println(s"Created ${tableName}")

  // mutable for the sake of simplicity
  val CheckpointData = new mutable.HashMap[Path, String]()

  def get(key: Path) = CheckpointData.get(key)

  // Fail-fast for this, if the method is called, it means that the caller expects to see a value
  // and if it's not there, something is wrong
  def getExistingEntry(key: Path) = CheckpointData(key)

  def put(key: Path, value: String) = CheckpointData.put(key, value)

  def remove(key: Path) = CheckpointData.remove(key)

  def allKeys = CheckpointData.keys
}
