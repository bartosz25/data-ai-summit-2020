package com.waitingforcode.statestore

import java.io.File

case class MapDBStateStoreNamingFactory(checkpointStorePath: String, localStorePath: String,
                                       operatorId: Long, partitionNumber: Int, stateStoreName: String) {
  new File(checkpointStorePath).mkdirs()
  new File(localStorePath).mkdirs()

  val allEntriesFile = s"${localStorePath}/all-entries-${stateStoreName}-${operatorId}-${partitionNumber}.db"

  def localDeltaForDelete(version: Long) = deleteFile(localStorePath, version)

  def checkpointDeltaForDelete(version: Long) = deleteFile(checkpointStorePath, version)

  def localDeltaForUpdate(version: Long) = updateFile(localStorePath, version)

  def checkpointDeltaForUpdate(version: Long) = updateFile(checkpointStorePath, version)

  def localSnapshot(version: Long) = snapshotFile(localStorePath, version)
  def checkpointSnapshot(version: Long) = snapshotFile(checkpointStorePath, version)

  private def updateFile(dir: String, version: Long) = {
    new File(s"${dir}/${version}").mkdirs()
    s"${dir}/${version}/updates-${stateStoreName}-${operatorId}-${partitionNumber}.db"
  }
  private def deleteFile(dir: String, version: Long) = {
    new File(s"${dir}/${version}").mkdirs()
    s"${dir}/${version}/deletes-${stateStoreName}-${operatorId}-${partitionNumber}.db"
  }
  private def snapshotFile(dir: String, version: Long) = s"${dir}/${version}/snapshot-${stateStoreName}-${operatorId}-${partitionNumber}.db"
}
