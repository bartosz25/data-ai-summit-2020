package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreId, StateStoreMetrics, UnsafeRowPair}
import org.apache.spark.sql.types.StructType
import org.mapdb.{DB, HTreeMap, Serializer, Store}

import scala.collection.JavaConverters.{asScalaSetConverter, iterableAsScalaIterableConverter}

class MapDBStateStore(previousVersion: Long, val id: StateStoreId,
                      checkpointStorePath: String, localSnapshotPath: String,
                      db: DB, mapWithAllEntries: HTreeMap[Array[Byte], Array[Byte]],
                      keySchema: StructType, valueSchema: StructType) extends StateStore {

  private var isCommitted = false
  val version = previousVersion + 1
  private var numberOfKeys = 0L

  private val versionMap = db
    .hashMap(s"state-delta-update-${version}", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
    .createOrOpen()
  private val deletesMap = db
    .hashSet(s"state-delta-delete-${version}", Serializer.BYTE_ARRAY)
    .createOrOpen()

  override def get(key: UnsafeRow): UnsafeRow = {
    val keyInBytes = key.getBytes
    val valueBytes = Option(versionMap.get(keyInBytes)).getOrElse(mapWithAllEntries.get(keyInBytes))
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
    println(s"Putting the key ${key} >> ${value}")
    // The key was already removed from the `mapWithAllEntries`
    versionMap.put(key.getBytes, value.getBytes)
  }

  override def remove(key: UnsafeRow): Unit = {
    val keyInBytes = key.getBytes
    deletesMap.add(keyInBytes)
    versionMap.remove(keyInBytes)
    mapWithAllEntries.remove(keyInBytes)
  }

  override def commit(): Long = {
    println(s"Committing the state for ${id.partitionId}")
    db.commit()
    numberOfKeys += versionMap.getKeys.asScala.size.toLong
    numberOfKeys += mapWithAllEntries.getKeys.asScala.size.toLong

    checkpointStoreFiles(versionMap.getStores.toSet, s"delta-${version}-updates")
    // BK: no idea why we cannot get the stores directly for the set?
    checkpointStoreFiles(deletesMap.getMap.getStores.toSet, s"delta-${version}-deletes")

    versionMap.getEntries.asScala.foreach(deltaMapEntry => {
      mapWithAllEntries.put(deltaMapEntry.getKey, deltaMapEntry.getValue)
      versionMap.remove(deltaMapEntry.getKey)
    })

    // TODO: make it dynamic
    if (version % 5 == 0) {
      // if the snapshot check is reached, save allMaps too and thanks to that,
      // the maintenance thread will simply take a copy of that file and put it to the DFS!
      // That's the simplest way I found ,feel free to share if you find a more efficient alternative
      dumpMapToFile(localSnapshotPath)(mapWithAllEntries.getStores.toSet, s"snapshot-${version}")
    }
    isCommitted = true
    version
  }

  private def dumpMapToFile(outputDir: String)(stores: Set[Store], filePrefix: String) = {
    stores.foreach(store => {
      store.getAllFiles.asScala.foreach(fileToCheckpointFullPath => {
        val sourceFileName = fileToCheckpointFullPath.split("/").last
        FileUtils.copyFile(new File(fileToCheckpointFullPath),
          new File(s"${checkpointStorePath}/${filePrefix}/${sourceFileName}"))
      })
    })
  }

  private def checkpointStoreFiles(stores: Set[Store], filePrefix: String) = dumpMapToFile(checkpointStorePath) _

  override def abort(): Unit = {
    println(s"Aborting the state store for ${version}")
    db.rollback()
    isCommitted = false
  }

  override def iterator(): Iterator[UnsafeRowPair] = {
    val unsafeRowPair = new UnsafeRowPair()
    Seq(versionMap, mapWithAllEntries).flatMap(mapToTransform => mapToTransform.getEntries.asScala.map(entry => {
      val key = new UnsafeRow(keySchema.fields.length)
      key.pointTo(entry.getKey, entry.getKey.length)
      val value = convertValueToUnsafeRow(entry.getValue)
      // TODO: using an UnsafeRowPair outside the mapper comes from the default state ==> WHY?
      unsafeRowPair.withRows(key, value)
    })).toIterator
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
