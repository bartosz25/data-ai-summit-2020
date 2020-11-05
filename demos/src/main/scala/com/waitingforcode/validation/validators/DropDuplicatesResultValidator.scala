package com.waitingforcode.validation.validators

import com.waitingforcode.stateful.DropDuplicatesStatefulAppConfig

object DropDuplicatesResultValidator extends ResultValidator[EventWithIdAndValue] {
  override val inputDir = DropDuplicatesStatefulAppConfig.outputDir
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