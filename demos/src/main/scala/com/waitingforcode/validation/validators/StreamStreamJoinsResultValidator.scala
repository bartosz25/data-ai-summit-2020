package com.waitingforcode.validation.validators

import com.waitingforcode.stateful.StreamToStreamJoinStatefulAppConfig

object StreamStreamJoinsResultValidator extends ResultValidator[AdClickJoined] {
  override val inputDir: String = StreamToStreamJoinStatefulAppConfig.outputDir
  override val validators: Iterator[Seq[AdClickJoined] => Boolean] = Iterator(
    // 0, 1
    inputRows => inputRows.isEmpty,
    inputRows => inputRows.isEmpty,
    // 2
    inputRows => {
      inputRows.size == 2 && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:26:00.000+02:00", click_time = "2020-05-05T10:25:10.000+02:00",
        ad_id = 1, clicks_ad_id = 1
      )) && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:26:00.000+02:00", click_time = "2020-05-05T10:26:10.000+02:00",
        ad_id = 2, clicks_ad_id = 2
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
        ad_id = 7, clicks_ad_id = 7
      )) && inputRows.contains(AdClickJoined(
        event_time = "2020-05-05T10:57:40.000+02:00", click_time = "2020-05-05T10:57:40.000+02:00",
        ad_id = 8, clicks_ad_id = 8
      ))
    }
  )
}