package com.waitingforcode.validation

import java.io.File
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.waitingforcode.stateful.UserClicks
import com.waitingforcode.{OutputDirDropDuplicates, OutputDirFlatMapGroupsWithState, OutputDirGlobalLimit, OutputDirMapGroupsWithState, OutputDirStreamStreamJoins}
import org.apache.commons.io.FileUtils

import scala.reflect.ClassTag

object ExpectedResults extends App {

  val Expectations: Map[String, ResultValidator[_]] = Map(
    //    OutputDirAggregation -> true,
    OutputDirStreamStreamJoins -> StreamStreamJoinsResultValidator,
    OutputDirMapGroupsWithState -> MapGroupsWithStateResultValidator,
    OutputDirFlatMapGroupsWithState -> FlatMapGroupsWithStateResultValidator,
    OutputDirGlobalLimit -> GlobalLimitResultValidator,
    OutputDirDropDuplicates -> DropDuplicatesResultValidator
    //OutputDirMultipleStatefulOperations -> true,
    //OutputDirWindowsWatermark -> true
  )

  Expectations.values.foreach(validator => {
    val isValid = validator.validate()
    println(s"isValid=${isValid}")
  })

}


abstract class ResultValidator[T:ClassTag] {
  val jsonObjectMapper = new ObjectMapper()
  jsonObjectMapper.registerModule(DefaultScalaModule)

  val inputDir: String
  val validators: Iterator[Seq[T] => Boolean]

  def validate(): Boolean = {
    println(s"Validating ${inputDir}")
    val microBatchDirs = new File(inputDir).listFiles.filter(_.isDirectory)
      .sortBy(file => file.getName)

    var validationRule = 1
    val validationResults = microBatchDirs.map(microBatchDir => {
      val validationMethod = validators.next()
      val jsonFiles = microBatchDir.listFiles
        .filter(fileCandidate => {
          fileCandidate.isFile && fileCandidate.getName.endsWith("json")
        })
      val jsonLinesFromFiles = jsonFiles.flatMap(jsonDataFile => {
        import scala.collection.JavaConverters._
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

object DropDuplicatesResultValidator extends ResultValidator[EventWithIdAndValue] {
  override val inputDir = OutputDirDropDuplicates
  override val validators: Iterator[Seq[EventWithIdAndValue] => Boolean] = Iterator(
    // 0
    inputRows => inputRows.isEmpty,
    // 1
    inputRows => {
      inputRows.size == 1 && inputRows(0) == EventWithIdAndValue("2020-05-05T01:22:00.000+02:00", 1, 1)
    },
    // 2
    inputRows => inputRows.isEmpty,
    // 3
    inputRows => {
      inputRows.size == 1 && inputRows(0) == EventWithIdAndValue("2020-05-05T03:22:00.000+02:00", 2, 1)
    },
    // 4
    inputRows => inputRows.isEmpty,
    // 5
    inputRows => inputRows.isEmpty,
    // 6
    inputRows => inputRows.isEmpty,
    // 7
    inputRows => inputRows.isEmpty
  )
}
object GlobalLimitResultValidator extends ResultValidator[EventWithIdAndValue] {
  override val inputDir: String = OutputDirGlobalLimit
  override val validators: Iterator[Seq[EventWithIdAndValue] => Boolean] = Iterator(
    // limit is not deterministic, so let's assert only on the rows size
    inputRows => inputRows.size == 2,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty
  )
}
object FlatMapGroupsWithStateResultValidator extends ResultValidator[UserClicks] {
  override val inputDir: String = OutputDirFlatMapGroupsWithState
  override val validators: Iterator[Seq[UserClicks] => Boolean] = Iterator(
    // 0, 1
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 2
    inputRows => {
      inputRows.size == 1 && inputRows(0) == UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:26:00.000"),
        clicksByType = Map("singleClick" -> 2))
    },
    // 3
    inputRows => inputRows.isEmpty,
    // 4
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin="user2",
        lastClick = Timestamp.valueOf("2020-05-05 01:31:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:41:00.000"),
        clicksByType = Map("doubleClick" -> 1)))
    },
    // 5
    inputRows => inputRows.isEmpty,
    // 6
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin="user2",
        lastClick = Timestamp.valueOf("2020-05-05 01:56:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:42:00.000"),
        clicksByType = Map("singleClick" -> 1)))
    },
    // 7, 8
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 9
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin = "user3",
        lastClick = Timestamp.valueOf("2020-05-05 02:02:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin = "user4",
        lastClick = Timestamp.valueOf("2020-05-05 02:16:00.000"),
        clicksByType = Map("singleClick" -> 1)))
    },
  )
}
object MapGroupsWithStateResultValidator extends ResultValidator[UserClicks] {
  override val inputDir: String = OutputDirMapGroupsWithState
  override val validators: Iterator[Seq[UserClicks] => Boolean] = Iterator(
    // 0, 1
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 2
    inputRows => {
      inputRows.size == 1 && inputRows(0) == UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:26:00.000"),
        clicksByType = Map("singleClick" -> 2))
    },
    // 3
    inputRows => inputRows.isEmpty,
    // 4
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin="user2",
        lastClick = Timestamp.valueOf("2020-05-05 01:31:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:41:00.000"),
        clicksByType = Map("doubleClick" -> 1)))
    },
    // 5
    inputRows => inputRows.isEmpty,
    // 6
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin="user2",
        lastClick = Timestamp.valueOf("2020-05-05 01:56:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin="user1",
        lastClick = Timestamp.valueOf("2020-05-05 01:42:00.000"),
        clicksByType = Map("singleClick" -> 1)))
    },
    // 7, 8
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 9
    inputRows => {
      inputRows.size == 2 && inputRows.contains(UserClicks(userLogin = "user3",
        lastClick = Timestamp.valueOf("2020-05-05 02:02:00.000"),
        clicksByType = Map("doubleClick" -> 1))) && inputRows.contains(UserClicks(userLogin = "user4",
        lastClick = Timestamp.valueOf("2020-05-05 02:16:00.000"),
        clicksByType = Map("singleClick" -> 1)))
    },
  )
}
object StreamStreamJoinsResultValidator extends ResultValidator[AdClickJoined] {
  override val inputDir: String = OutputDirStreamStreamJoins
  override val validators: Iterator[Seq[AdClickJoined] => Boolean] = Iterator(
    // 0, 1
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 2
    inputRows => {
      inputRows.size == 2 && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:26:00.000+02:00", click_time = "2020-05-05T10:25:10.000+02:00",
        ad_id = 1, click_ad_id = 1
      )) && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:26:00.000+02:00", click_time = "2020-05-05T10:26:10.000+02:00",
        ad_id = 2, click_ad_id = 2
      ))
    },
    // 3, 4, 5, 6, 7
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 8
    inputRows => {
      inputRows.size == 2 && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:57:00.000+02:00", click_time = "2020-05-05T10:47:00.000+02:00",
        ad_id = 7, click_ad_id = 7
      )) && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:57:40.000+02:00", click_time = "2020-05-05T10:57:40.000+02:00",
        ad_id = 8, click_ad_id = 8
      ))
    }
  )
}
case class EventWithIdAndValue(event_time: String, id: Int, value: Int)
case class AdClickJoined(event_time: String, ad_id: Int, click_time: String, click_ad_id: Int)