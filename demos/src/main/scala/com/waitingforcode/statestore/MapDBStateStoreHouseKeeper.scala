package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils

class MapDBStateStoreHouseKeeper(remoteCheckpointPath: String, localSnapshotPath: String) {

  def doCheckpointing(stateStoreVersion: Long) = {
    FileUtils.copyDirectory(new File(s"${localSnapshotPath}/snapshot-${stateStoreVersion}"),
      new File(s"${remoteCheckpointPath}/snapshot-${stateStoreVersion}")
    )

    FileUtils.deleteDirectory(new File(s"${remoteCheckpointPath}/delta-${stateStoreVersion}-updates"))
    FileUtils.deleteDirectory(new File(s"${remoteCheckpointPath}/delta-${stateStoreVersion}-deletes"))
  }


}
