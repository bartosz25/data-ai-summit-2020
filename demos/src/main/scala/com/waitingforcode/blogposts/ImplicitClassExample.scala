package com.waitingforcode.blogposts

object ImplicitClassExample extends App {

  implicit class StringDotPrinter(textToPrint: String) {
    def printWithDots = println(textToPrint.split("").mkString("."))
  }

  "abc".printWithDots

}
