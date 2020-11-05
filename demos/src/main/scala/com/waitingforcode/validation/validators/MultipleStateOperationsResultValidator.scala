package com.waitingforcode.validation.validators

import com.waitingforcode.stateful.MultipleStateOperationsStatefulAppConfig

object MultipleStateOperationsResultValidator extends ResultValidator[WindowFunctionResult] {
  override val inputDir = MultipleStateOperationsStatefulAppConfig.outputDir
  override val validators: Iterator[Seq[WindowFunctionResult] => Boolean] = Iterator(
    // 0
    inputRows => inputRows.isEmpty,
    // 1
    inputRows => {
      inputRows.size == 1 &&
        inputRows(0) == WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:20:00.000+02:00", end = "2020-05-05T01:30:00.000+02:00"),
          sum = 4)
    },
    // 2
    inputRows => inputRows.isEmpty,
    // 3
    inputRows => {
      inputRows.size == 2 &&
        inputRows.contains(WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:20:00.000+02:00", end = "2020-05-05T01:30:00.000+02:00"),
          sum = 5)) &&
        inputRows.contains(WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:40:00.000+02:00", end = "2020-05-05T01:50:00.000+02:00"),
          sum = 3))
    },
    // 4
    inputRows => inputRows.isEmpty,
    // 5
    inputRows => {
      inputRows.size == 1 &&
        inputRows(0) == WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:40:00.000+02:00", end = "2020-05-05T01:50:00.000+02:00"),
          sum = 10)
    },
    // 6
    inputRows => inputRows.isEmpty,
    // 7
    inputRows => {
      inputRows.size == 1 &&
        inputRows(0) == WindowFunctionResult(
          WindowFunctionWindow(start = "2020-05-05T01:20:00.000+02:00", end = "2020-05-05T01:30:00.000+02:00"),
          sum = 9)
    }
  )
}
