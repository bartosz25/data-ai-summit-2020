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
    if (valueBytes == null) {
      null
    } else {
      convertValueToUnsafeRow(valueBytes)
    }
  }

  /**
   * In the ScalaDoc ([[org.apache.spark.sql.execution.streaming.state.StateStore.put]])
   * you can read that the key and value can be reused, so it's better
   * to call .copy() on them. However, we're using here the bytes, so it's safe to call
   * .getBytes without .copy().
   * Check this [[com.waitingforcode.blogposts.UnsafeRowBytesCopyNeeded]] to see it in action.
   *
   * We're also removing the key from [[mapWithAllEntries]] to do not multiply the storage space.
   * The updated key will be put back to this map in the [[commit()]] method.
   */
  override def put(key: UnsafeRow, value: UnsafeRow): Unit = {
    updatesFromVersion.put(key.getBytes, value.getBytes)
    mapWithAllEntries.remove(key.getBytes)
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

    def commitLocalStore(localDb: DB, localPath: String, checkpointPath: String): Unit = {
      localDb.commit() // Commit is a required marker to consider the .db file as fully valid
      logInfo(s"Writing updates to ${localPath}")
      FileUtils.copyFile(new File(localPath), new File(
        checkpointPath
      ))
      new File(localPath).delete()
    }
    commitLocalStore(updatesFromVersionDb, updatesFileFullPath, namingFactory.checkpointDeltaForUpdate(version))
    commitLocalStore(deletesFromVersionDb, deletesFileFullPath, namingFactory.checkpointDeltaForDelete(version))

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
    mapAllEntriesDb.close()
    deletesFromVersion.clear()
    deletesFromVersionDb.close()
    updatesFromVersion.clear()
    updatesFromVersionDb.close()
    isCommitted = false
  }

  override def iterator(): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    def setKeyAndValueToUnsafeRowPair(entry: java.util.Map.Entry[Array[Byte], Array[Byte]]): UnsafeRowPair = {
      val key = new UnsafeRow(keySchema.fields.length)
      key.pointTo(entry.getKey, entry.getKey.length)
      val value = convertValueToUnsafeRow(entry.getValue)
      unsafeRowPair.withRows(key, value)
    }
    updatesFromVersion.getEntries.asScala.toIterator.map(entry => {
      setKeyAndValueToUnsafeRowPair(entry)
    }) ++ mapWithAllEntries.getEntries.asScala.toIterator.map(entry => {
      setKeyAndValueToUnsafeRowPair(entry)
    })
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