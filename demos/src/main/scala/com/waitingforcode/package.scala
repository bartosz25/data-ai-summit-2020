package com

package object waitingforcode {

  val BaseProjectDir = "/tmp/data+ai/main"
  val OutputDirAggregation = s"${BaseProjectDir}/aggregations/output"
  val OutputDirDropDuplicates = s"${BaseProjectDir}/drop_duplicates/output"
  val OutputDirFlatMapGroupsWithState = s"${BaseProjectDir}/flat_map_groups_state/output"
  val OutputDirGlobalLimit = s"${BaseProjectDir}/aggregations/global_limit/output"
  val OutputDirMapGroupsWithState = s"${BaseProjectDir}/map_groups_with_state/output"
  val OutputDirMultipleStatefulOperations = s"${BaseProjectDir}/multiple_stateful_ops/output"
  val OutputDirStreamStreamJoins = s"${BaseProjectDir}/stream_stream_joins/output"
  val OutputDirWindowsWatermark = s"${BaseProjectDir}/windows_watermark/output"

  val AllOutputLocations = Seq(OutputDirAggregation, OutputDirDropDuplicates, OutputDirFlatMapGroupsWithState,
    OutputDirGlobalLimit, OutputDirMapGroupsWithState, OutputDirMultipleStatefulOperations,
    OutputDirStreamStreamJoins, OutputDirWindowsWatermark)

}
