package com.waitingforcode.statestore

case class MapDBStateStoreNamingFactory(checkpointStorePath: String, localStorePath: String,
                                       operatorId: Long, partitionNumber: Int) {

  val allEntriesFile = s"${localStorePath}/all-entries.db"

  def localDeltaForDelete(version: Long) = deleteFile(localStorePath, version)

  def checkpointDeltaForDelete(version: Long) = deleteFile(checkpointStorePath, version)

  def localDeltaForUpdate(version: Long) = updateFile(localStorePath, version)

  def checkpointDeltaForUpdate(version: Long) = updateFile(checkpointStorePath, version)

  def localSnapshot(version: Long) = snapshotFile(localStorePath, version)
  def checkpointSnapshot(version: Long) = snapshotFile(checkpointStorePath, version)

  private def updateFile(dir: String, version: Long) = s"${dir}/${version}/updates-${operatorId}-${partitionNumber}.db"
  private def deleteFile(dir: String, version: Long) = s"${dir}/${version}/deletes-${operatorId}-${partitionNumber}.db"
  private def snapshotFile(dir: String, version: Long) = s"${dir}/${version}/snapshot-${operatorId}-${partitionNumber}.db"
}
