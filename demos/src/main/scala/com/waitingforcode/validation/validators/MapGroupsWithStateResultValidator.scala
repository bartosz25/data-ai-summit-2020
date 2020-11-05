package com.waitingforcode.validation.validators

import java.sql.Timestamp

import com.waitingforcode.stateful.{MapGroupsWithStateStatefulAppConfig, UserClicks}

object MapGroupsWithStateResultValidator extends ResultValidator[UserClicks] {
  override val inputDir: String = MapGroupsWithStateStatefulAppConfig.outputDir
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