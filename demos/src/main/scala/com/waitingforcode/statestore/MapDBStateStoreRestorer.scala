package com.waitingforcode.statestore

import java.io.File

import com.waitingforcode.statestore.MapDBStateStore.EntriesName
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.mapdb.{DBMaker, HTreeMap, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter

class MapDBStateStoreRestorer(namingFactory: MapDBStateStoreNamingFactory, lastSnapshotVersion: Long,
                                   stateStoreVersionToRestore: Long, stateStoreId: StateStoreId) extends Logging {

  private val uniqueId = s"${stateStoreId.partitionId}-${stateStoreId.operatorId}-${stateStoreId.storeName}"
  private val temporaryRestoreDbFile = s"/tmp/restore-${lastSnapshotVersion}-${uniqueId}-${System.currentTimeMillis()}"
  new File(temporaryRestoreDbFile).delete()
  logInfo(s"Creating file ${temporaryRestoreDbFile}")
  private val db = DBMaker
    .fileDB(temporaryRestoreDbFile)
    .fileMmapEnableIfSupported()
    .make()

  private val allEntriesMap = db.hashMap(EntriesName,
    Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen()

  def restoreFromSnapshot(): this.type = {
    if (lastSnapshotVersion > 0) {
      logInfo(s"Restoring snapshot ${lastSnapshotVersion}")
      val db = DBMaker
        .fileDB(namingFactory.checkpointSnapshot(lastSnapshotVersion))
        .fileMmapEnableIfSupported()
        .make()
      val allStates = db.hashMap(EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
        .createOrOpen()
      allEntriesMap.putAll(allStates)
    }
    this
  }

  def applyUpdatesAndDeletes(): this.type = {
    (lastSnapshotVersion+1 to stateStoreVersionToRestore).foreach(deltaToLoad => {
      logInfo(s"Restoring delta state store ${deltaToLoad}")
      val updates = restoreUpdatesFromDelta(deltaToLoad)
      allEntriesMap.putAll(updates)
      val deletes = restoreDeletesFromDelta(deltaToLoad)
      deletes.asScala.foreach(entry => {
        allEntriesMap.remove(entry)
      })
    })
    this
  }

  def getAllEntriesMap: HTreeMap[Array[Byte], Array[Byte]] = {
    allEntriesMap
  }

  def restoreUpdatesFromDelta(deltaVersion: Long) = {
    val dbUpdate = DBMaker
      .fileDB(namingFactory.checkpointDeltaForUpdate(deltaVersion))
      .fileMmapEnableIfSupported()
      .make()
    dbUpdate.hashMap(EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen()
  }

  def restoreDeletesFromDelta(deltaVersion: Long) = {
    val dbRemove = DBMaker
      .fileDB(namingFactory.checkpointDeltaForDelete(deltaVersion))
      .fileMmapEnableIfSupported()
      .make()
    dbRemove.hashSet(EntriesName, Serializer.BYTE_ARRAY).createOrOpen()
  }

}
