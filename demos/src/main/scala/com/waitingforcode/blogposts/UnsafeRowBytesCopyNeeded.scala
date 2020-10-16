package com.waitingforcode.blogposts

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

import scala.collection.mutable

object UnsafeRowBytesCopyNeeded extends App {

  val unsafeRows = new mutable.ListBuffer[UnsafeRow]()
  val bytesFromUnsafeRow = new mutable.ListBuffer[String]()
  val bytes = "123".getBytes("utf-8")
  val unsafeRowFromBytes = new UnsafeRow(1)
  unsafeRowFromBytes.pointTo(bytes, bytes.length)
  bytesFromUnsafeRow.append(new String(unsafeRowFromBytes.getBytes))
  unsafeRows.append(unsafeRowFromBytes)

  val nextBytes = "345".getBytes("utf-8")
  unsafeRowFromBytes.pointTo(nextBytes, nextBytes.length)
  bytesFromUnsafeRow.append(new String(unsafeRowFromBytes.getBytes))
  unsafeRows.append(unsafeRowFromBytes)

  // `bytesFromUnsafeRow` keeps 2 different values, normal since it
  // extracts the bytes from every `UnsafeRow` at the given moment
  println(bytesFromUnsafeRow)
  // `unsafeRows` keeps only 1 value, since the `UnsafeRow` is mutable
  println(unsafeRows)
  unsafeRows.foreach(row => println(new String(row.getBytes)))

}
