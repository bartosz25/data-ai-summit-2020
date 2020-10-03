package com.waitingforcode.checkpoint.io

import org.apache.hadoop.fs.FSInputStream

class MemorySeekableInputStream(input: Array[Byte]) extends FSInputStream {

  private var position = 0L

  override def seek(newPosition: Long): Unit = {
    position = newPosition
  }

  override def getPos: Long = position

  override def seekToNewSource(targetPos: Long): Boolean = false

  override def read(): Int = {
    val positionToReturn = if (position < input.length) {
      input(position.toInt)
    } else {
      -1
    }
    position += 1
    positionToReturn
  }
}

class MemorySeekableEmptyInputStream extends FSInputStream {
  override def seek(pos: Long): Unit = {}

  override def getPos: Long = 0

  override def seekToNewSource(targetPos: Long): Boolean = false

  override def read(): Int = -1
}