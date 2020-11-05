package com.waitingforcode.validation.validators

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils
import scala.collection.JavaConverters._

import scala.reflect.ClassTag

abstract class ResultValidator[T:ClassTag] {
  val jsonObjectMapper = new ObjectMapper()
  jsonObjectMapper.registerModule(DefaultScalaModule)

  val inputDir: String
  val validators: Iterator[Seq[T] => Boolean]

  def validate(): Boolean = {
    println(s"Validating ${inputDir}")
    val microBatchDirs = new File(inputDir).listFiles.filter(_.isDirectory)
      .sortBy(file => file.getName.split("/").last.toInt)

    var validationRule = 1
    val validationResults = microBatchDirs.map(microBatchDir => {
      val validationMethod = validators.next()
      val jsonFiles = microBatchDir.listFiles
        .filter(fileCandidate => {
          fileCandidate.isFile && fileCandidate.getName.endsWith("json")
        })
      val jsonLinesFromFiles = jsonFiles.flatMap(jsonDataFile => {
        FileUtils.readLines(jsonDataFile).asScala
      })
      val objectsToValidate = jsonLinesFromFiles
        .map(jsonLine => {
          jsonObjectMapper.readValue(jsonLine,
            implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
          )
        })
      val validationResult = validationMethod(objectsToValidate)
      if (!validationResult) {
        println(s"${this} Error occurred for rule ${validationRule}")
        println(s"Got ${jsonLinesFromFiles.mkString("\n")}")
      }
      validationRule += 1
      validationResult
    })
    validationResults.forall(result => result)
  }
}