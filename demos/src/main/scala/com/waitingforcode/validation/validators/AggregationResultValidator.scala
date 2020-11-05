package com.waitingforcode.validation.validators

import com.waitingforcode.stateful.AggregationStatefulAppConfig

object AggregationResultValidator extends ResultValidator[WindowFunctionResult] {
  override val inputDir = AggregationStatefulAppConfig.outputDir
  override val validators: Iterator[Seq[WindowFunctionResult] => Boolean] = Iterator(
    // 0, 1, 2, 3
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 4
    inputRows => {
      inputRows.size == 1 &&
        inputRows(0) == WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:20:00.000+02:00", end = "2020-05-05T01:30:00.000+02:00"),
          sum = 4)
    },
    // 5, 6, 7, 8, 9, 10
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 11
    inputRows => {
      inputRows.size == 1 && inputRows(0) == WindowFunctionResult(
        WindowFunctionWindow(start = "2020-05-05T01:30:00.000+02:00", end = "2020-05-05T01:40:00.000+02:00"),
        sum = 18)
    },
    // 12
    inputRows => inputRows.isEmpty,
    // 13
    inputRows => {
      inputRows.size == 1 && inputRows(0) == WindowFunctionResult(
        WindowFunctionWindow(start = "2020-05-05T01:40:00.000+02:00", end = "2020-05-05T01:50:00.000+02:00"),
        sum = 14)
    },
  )
}
