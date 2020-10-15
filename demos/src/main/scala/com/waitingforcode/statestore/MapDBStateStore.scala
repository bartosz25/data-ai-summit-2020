package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId, StateStoreMetrics, UnsafeRowPair}
import org.apache.spark.sql.types.StructType
import org.mapdb.{DB, DBMaker, HTreeMap, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter

class MapDBStateStore(previousVersion: Long, val id: StateStoreId,
                      namingFactory: MapDBStateStoreNamingFactory,
                      performLocalSnapshot: Boolean,
                      mapAllEntriesDb: DB, mapWithAllEntries: HTreeMap[Array[Byte], Array[Byte]],
                      keySchema: StructType, valueSchema: StructType) extends StateStore with Logging {

  private var isCommitted = false
  val version = previousVersion + 1
  private var numberOfKeys = 0L

  private val updatesFileFullPath = namingFactory.localDeltaForUpdate(version)
  private val updatesFromVersionDb = DBMaker
    .fileDB(updatesFileFullPath)
    .fileMmapEnableIfSupported()
    .make()
  private val updatesFromVersion = updatesFromVersionDb
    .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
    .createOrOpen()

  private val deletesFileFullPath = namingFactory.localDeltaForDelete(version)
  private val deletesFromVersionDb = DBMaker
    .fileDB(deletesFileFullPath)
    .fileMmapEnableIfSupported()
    .make()
  private val deletesFromVersion = deletesFromVersionDb
    .hashSet(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY)
    .createOrOpen()

  override def get(key: UnsafeRow): UnsafeRow = {
    val keyInBytes = key.getBytes
    val valueBytes = Option(updatesFromVersion.get(keyInBytes)).getOrElse(mapWithAllEntries.get(keyInBytes))
    mapWithAllEntries.remove(keyInBytes)
    if (valueBytes == null) {
      null
    } else {
      convertValueToUnsafeRow(valueBytes)
    }
  }

  override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    // TODO: do I need the .copy() here? After all, I'm copying the bytes and they shouldn't be
    //       conflicted between runs
    // The key was already removed from the `mapWithAllEntries`
    updatesFromVersion.put(key.getBytes, value.getBytes)
    mapWithAllEntries.remove(key.getBytes)
    // do not put to the mapAllEntries because we want to preserve the updates in the
    // updatesFromVersion and do not duplicate the entries
  }

  override def remove(key: UnsafeRow): Unit = {
    val keyInBytes = key.getBytes
    deletesFromVersion.add(keyInBytes)
    updatesFromVersion.remove(keyInBytes)
    mapWithAllEntries.remove(keyInBytes)
  }

  override def commit(): Long = {
    logInfo(s"Committing the state for ${id.partitionId}")
    numberOfKeys += updatesFromVersion.getKeys.asScala.size.toLong
    numberOfKeys += mapWithAllEntries.getKeys.asScala.size.toLong

    updatesFromVersionDb.commit() // Commit is a required marker to consider the .db file as fully valid
    logInfo(s"Writing updates to ${updatesFileFullPath}")
    FileUtils.copyFile(new File(updatesFileFullPath), new File(
      namingFactory.checkpointDeltaForUpdate(version)
    ))
    deletesFromVersionDb.commit()
    logInfo(s"Writing deletes to ${deletesFileFullPath}")
    FileUtils.copyFile(new File(deletesFileFullPath), new File(
      namingFactory.checkpointDeltaForDelete(version)
    ))

    updatesFromVersion.getEntries.asScala.foreach(entry => {
      mapWithAllEntries.put(entry.getKey, entry.getValue)
      updatesFromVersion.remove(entry.getKey)
    })
    mapAllEntriesDb.commit()
    if (performLocalSnapshot) {
      // if the snapshot check is reached, save allMaps too and thanks to that,
      // the maintenance thread will simply take a copy of that file and put it to the DFS!
      // That's the simplest way I found ,feel free to share if you find a more efficient alternative
      val snapshotFileFullPath = namingFactory.localSnapshot(version)
      logInfo(s"Taking the state snapshot to ${snapshotFileFullPath}")
      FileUtils.copyFile(
        new File(namingFactory.allEntriesFile), new File(snapshotFileFullPath)
      )
    }
    isCommitted = true
    version
  }

  override def abort(): Unit = {
    logWarning(s"Aborting the state store for ${version}")
    mapAllEntriesDb.rollback()
    deletesFromVersionDb.close()
    updatesFromVersionDb.close()
    isCommitted = false
  }

  override def iterator(): Iterator[UnsafeRowPair] = {
    Seq(updatesFromVersion, mapWithAllEntries).flatMap(mapToTransform => {
      val unsafeRowPair = new UnsafeRowPair()
      mapToTransform.getEntries.asScala.map(entry => {
        val key = new UnsafeRow(keySchema.fields.length)
        key.pointTo(entry.getKey, entry.getKey.length)
        val value = convertValueToUnsafeRow(entry.getValue)
        // TODO: using an UnsafeRowPair outside the mapper comes from the default state ==> WHY?
        unsafeRowPair.withRows(key, value)
      })
    }).toIterator
  }

  override def metrics: StateStoreMetrics = {
    StateStoreMetrics(
      numKeys = numberOfKeys,
      memoryUsedBytes = -1L,
      customMetrics = Map.empty
    )
  }

  override def hasCommitted: Boolean = isCommitted

  private def convertValueToUnsafeRow(bytes: Array[Byte]) = convertBytesToUnsafeRow(bytes, valueSchema)
  private def convertBytesToUnsafeRow(bytes: Array[Byte], schema: StructType): UnsafeRow = {
    val unsafeRowFromBytes = new UnsafeRow(schema.fields.length)
    unsafeRowFromBytes.pointTo(bytes, bytes.length)
    unsafeRowFromBytes
  }
}

object MapDBStateStore {
  val EntriesName = "state-all-entries"
}