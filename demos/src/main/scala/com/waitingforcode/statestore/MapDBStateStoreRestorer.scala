package com.waitingforcode.statestore

import java.io.File

import com.waitingforcode.statestore.MapDBStateStore.EntriesName
import org.apache.spark.internal.Logging
import org.mapdb.{DBMaker, HTreeMap, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter

case class MapDBStateStoreRestorer(namingFactory: MapDBStateStoreNamingFactory, lastSnapshotVersion: Long,
                                   stateStoreVersionToRestore: Long) extends Logging {

  private val temporaryDbDir = s"/tmp/restore-${lastSnapshotVersion}"
  new File(temporaryDbDir).delete()
  private val db = DBMaker
    .fileDB(temporaryDbDir)
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
      logInfo(s"Loading ${deltaToLoad}")
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
