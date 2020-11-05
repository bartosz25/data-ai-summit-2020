package com.waitingforcode.validation.validators

import com.waitingforcode.stateful.GlobalLimitStatefulAppConfig

object GlobalLimitResultValidator extends ResultValidator[EventWithIdAndValue] {
  override val inputDir: String = GlobalLimitStatefulAppConfig.outputDir
  override val validators: Iterator[Seq[EventWithIdAndValue] => Boolean] = Iterator(
    // limit is not deterministic, so let's assert only on the rows size
    inputRows => inputRows.size == 2,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty
  )
}