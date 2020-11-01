package com.waitingforcode.stateful

import java.io.File

import com.waitingforcode.data.configuration.{AggregationDataGeneratorConfiguration, DataGenerationConfiguration, DropDuplicatesDataGeneratorConfiguration, FlatMapGroupsWithStateDataGeneratorConfiguration, GlobalLimitDataGeneratorConfiguration, MapGroupsWithStateDataGeneratorConfiguration, MultipleStateOperationsDataGeneratorConfiguration, StreamStreamJoinsAdsDataGeneratorConfiguration, StreamStreamJoinsClicksDataGeneratorConfiguration, WindowsWithWatermarkDataGeneratorConfiguration}
import org.apache.commons.io.FileUtils

sealed trait StatefulAppConfig {
  val name: String
  val appName: String
  val dataConfig: DataGenerationConfiguration
  private val baseDir = s"/tmp/data+ai/stateful/${name}"
  val checkpointDir = s"${baseDir}/checkpoint"
  val outputDir = s"${baseDir}/output"
  val mapDbLocalPath = s"${baseDir}/mapdb-local"
  val mapDbCheckpointPath = s"${baseDir}/mapdb-checkpoint"

  def cleanUpDirs = {
    FileUtils.deleteDirectory(new File(baseDir))
  }
}
object AggregationStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "aggregation_demo"
  override val appName: String = "[stateful] Aggregation demo"
  override val dataConfig: DataGenerationConfiguration = AggregationDataGeneratorConfiguration
}
object DropDuplicatesStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "drop_duplicates_demo"
  override val appName: String = "[stateful] Drop duplicates demo"
  override val dataConfig: DataGenerationConfiguration = DropDuplicatesDataGeneratorConfiguration
}
object FlatMapGroupsWithStateStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "flatmapgroupswithstate_demo"
  override val appName: String = "[stateful] flatMapGroupsWithState demo"
  override val dataConfig: DataGenerationConfiguration = FlatMapGroupsWithStateDataGeneratorConfiguration
}
object GlobalLimitStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "global_limit_demo"
  override val appName: String = "[stateful] Global limit demo"
  override val dataConfig: DataGenerationConfiguration = GlobalLimitDataGeneratorConfiguration
}
object MapGroupsWithStateStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "mapgroupswithstate_demo"
  override val appName: String = "[stateful] mapGroupsWithState demo"
  override val dataConfig: DataGenerationConfiguration = MapGroupsWithStateDataGeneratorConfiguration
}
object MultipleStateOperationsStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "multiple_states_demo"
  override val appName: String = "[stateful] Multiple state operations demo"
  override val dataConfig: DataGenerationConfiguration = MultipleStateOperationsDataGeneratorConfiguration
}
object WindowsWithWatermarkStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "window_demo"
  override val appName: String = "[stateful] Window demo"
  override val dataConfig: DataGenerationConfiguration = WindowsWithWatermarkDataGeneratorConfiguration
}

object StreamToStreamJoinStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "stream_strema_joins"
  override val appName: String = "[stateful] Stream-stream join demo"
  override val dataConfig: DataGenerationConfiguration = StreamStreamJoinsAdsDataGeneratorConfiguration

}
object JoinAdsStatefulAppConfig extends StatefulAppConfig {
  override val name: String = "multiple_states_demo"
  override val appName: String = "[stateful] Multiple state operations demo"
  override val dataConfig: DataGenerationConfiguration = StreamStreamJoinsClicksDataGeneratorConfiguration
}