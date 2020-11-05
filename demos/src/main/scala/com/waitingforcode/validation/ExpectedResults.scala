package com.waitingforcode.validation

import com.waitingforcode.validation.validators._

object ExpectedResults extends App {

  Seq(AggregationResultValidator, MultipleStateOperationsResultValidator, StreamStreamJoinsResultValidator, MapGroupsWithStateResultValidator,
    FlatMapGroupsWithStateResultValidator, GlobalLimitResultValidator,
    DropDuplicatesResultValidator).foreach(validator => {
    val isValid = validator.validate()
    println(s"isValid=${isValid}")
  })

}








