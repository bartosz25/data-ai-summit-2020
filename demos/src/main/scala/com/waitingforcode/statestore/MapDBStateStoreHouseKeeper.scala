package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging

class MapDBStateStoreHouseKeeper(namingFactory: MapDBStateStoreNamingFactory) extends Logging {

  def doCheckpointing(stateStoreVersion: Long) = {
    FileUtils.copyDirectory(new File(namingFactory.localSnapshot(stateStoreVersion)),
      new File(namingFactory.checkpointSnapshot(stateStoreVersion))
    )

    FileUtils.deleteDirectory(new File(namingFactory.checkpointDeltaForUpdate(stateStoreVersion)))
    FileUtils.deleteDirectory(new File(namingFactory.checkpointDeltaForDelete(stateStoreVersion)))
  }

  def deleteTooOldVersions(minVersionsToRetain: Long, lastCommittedVersion: Long) = {
    val earliestVersionToRetain = lastCommittedVersion - minVersionsToRetain
    var doesMapFileExist = earliestVersionToRetain > 0
    for (stateStoreVersion <- (earliestVersionToRetain-1 to 0) if doesMapFileExist) {
      val updatesFile = new File(namingFactory.checkpointDeltaForUpdate(stateStoreVersion))
      val deletesFile = new File(namingFactory.checkpointDeltaForDelete(stateStoreVersion))
      doesMapFileExist = updatesFile.exists() || deletesFile.exists()
      logInfo(s"[Cleanup][v#${stateStoreVersion}] updates flag=${updatesFile.exists()} / deletes flag=${deletesFile.exists()}")

      // We suppose it won't fail if the file doesn't exist
      updatesFile.delete()
      deletesFile.delete()
      new File(namingFactory.checkpointSnapshot(stateStoreVersion)).delete()
    }
  }

}
