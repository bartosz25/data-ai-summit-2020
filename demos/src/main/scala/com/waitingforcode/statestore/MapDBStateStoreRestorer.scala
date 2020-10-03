package com.waitingforcode.statestore

import java.io.File

import com.waitingforcode.statestore.MapDBStateStore.EntriesName
import org.mapdb.{DBMaker, HTreeMap, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter

case class MapDBStateStoreRestorer(checkpointDir: String, lastSnapshotVersion: Long, stateStoreVersionToRestore: Long) {

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
      println(s"Restoring snapshot ${lastSnapshotVersion}")
      val db = DBMaker
        .fileDB(s"${checkpointDir}/snapshot-${lastSnapshotVersion}")
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
      println(s"Loading ${deltaToLoad}")
      val updates = restoreUpdatesFromDelta(checkpointDir, deltaToLoad)
      allEntriesMap.putAll(updates)
      val deletes = restoreDeletesFromDelta(checkpointDir, deltaToLoad)
      deletes.asScala.foreach(entry => {
        allEntriesMap.remove(entry)
      })
    })
    this
  }

  def getAllEntriesMap: HTreeMap[Array[Byte], Array[Byte]] = {
    allEntriesMap
  }

  def restoreUpdatesFromDelta(checkpointDir: String, deltaVersion: Long) = {
    val dbUpdate = DBMaker
      .fileDB(s"${checkpointDir}/delta-${deltaVersion}-update")
      .fileMmapEnableIfSupported()
      .make()
    dbUpdate.hashMap(EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen()
  }

  def restoreDeletesFromDelta(checkpointDir: String, deltaVersion: Long) = {
    val dbRemove = DBMaker
      .fileDB(s"${checkpointDir}/delta-${deltaVersion}-delete")
      .fileMmapEnableIfSupported()
      .make()
    dbRemove.hashSet(EntriesName, Serializer.BYTE_ARRAY).createOrOpen()
  }

}
