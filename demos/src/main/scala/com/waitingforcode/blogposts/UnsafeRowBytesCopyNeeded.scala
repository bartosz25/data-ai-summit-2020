package com.waitingforcode.blogposts

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.UnsafeRowPair

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

object UnsafeRowPair extends App {
  println("--- Checking the materialized collection version ---")
  val unsafeRowPair = new UnsafeRowPair()
  val mappedUnsafeRowPairs = Seq(Seq("1", "2", "3"), Seq("4", "5", "6")).flatMap(numbers => {
    numbers.map(number => {
      val numberBytes = number.getBytes("utf-8")
      val unsafeRowNumber = new UnsafeRow(1)
      unsafeRowNumber.pointTo(numberBytes, numberBytes.length)
      unsafeRowPair.withRows(unsafeRowNumber, unsafeRowNumber)
    })
  })

  mappedUnsafeRowPairs.foreach(pair => {
    println(s"${new String(pair.key.getBytes)} ==> ${new String(pair.value.getBytes)}")
  })

  println("--- Checking the iterator version ---")
  val unsafeRowPairInternal = new UnsafeRowPair()
  def setKeyAndValueToUnsafeRowPair(number: String): UnsafeRowPair = {
    val numberBytes = number.getBytes("utf-8")
    val unsafeRowNumber = new UnsafeRow(1)
    unsafeRowNumber.pointTo(numberBytes, numberBytes.length)
    unsafeRowPairInternal.withRows(unsafeRowNumber, unsafeRowNumber)
  }
  val mappedUnsafeRowPairsInternal = Seq("1", "2", "3").toIterator.map(number => setKeyAndValueToUnsafeRowPair(number)) ++
    Seq("4", "5", "6").toIterator.map(number => setKeyAndValueToUnsafeRowPair(number))
  // This one retrieves the rows correctly
  // UnsafeRowPair is a shared buffer and since the iterator materializes
  // one item at a time, it always return the next row
  mappedUnsafeRowPairsInternal.foreach(pair => {
    println(s"${new String(pair.key.getBytes)} ==> ${new String(pair.value.getBytes)}")
  })

}