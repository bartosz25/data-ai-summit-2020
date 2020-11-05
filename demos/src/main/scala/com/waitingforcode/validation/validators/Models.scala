package com.waitingforcode.validation.validators


case class EventWithIdAndValue(event_time: String, id: Int, value: Int)

case class AdClickJoined(event_time: String, ad_id: Int, click_time: String, clicks_ad_id: Int)

case class WindowFunctionResult(window: WindowFunctionWindow, sum: Long)
case class WindowFunctionWindow(start: String, end: String)
