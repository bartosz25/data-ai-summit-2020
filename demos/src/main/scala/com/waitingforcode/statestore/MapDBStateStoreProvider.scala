package com.waitingforcode.statestore

import com.waitingforcode.statestore.MapDBStateStoreProvider.NoCommittedVersionFlag
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.types.StructType
import org.mapdb.{DBMaker, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter

class MapDBStateStoreProvider extends StateStoreProvider with Logging {

  private var keySchema: StructType = _
  private var valueSchema: StructType = _
  private var stateStoreIdValue: StateStoreId = _
  private var stateStoreConf: StateStoreConf = _
  private var lastCommittedVersion = NoCommittedVersionFlag
  private var houseKeeper: MapDBStateStoreHouseKeeper = _
  private var namingFactory: MapDBStateStoreNamingFactory = _

  private lazy val db = DBMaker
    .fileDB(namingFactory.allEntriesFile)
    .fileMmapEnableIfSupported()
    .transactionEnable()
    .make()

  private lazy val mapWithAllEntries =
    db.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen()

  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType,
                    keyIndexOrdinal: Option[Int], storeConfs: StateStoreConf,
                    hadoopConf: Configuration): Unit = {
    logInfo(s"Initializing the state store for ${stateStoreId}")
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.stateStoreIdValue = stateStoreId
    this.stateStoreConf = storeConfs
    this.namingFactory = MapDBStateStoreNamingFactory(
      localStorePath = storeConfs.confs("spark.sql.streaming.stateStore.mapdb.localPath"),
      checkpointStorePath = storeConfs.confs("spark.sql.streaming.stateStore.mapdb.checkpointPath"),
      operatorId = stateStoreId.operatorId,
      partitionNumber = stateStoreId.partitionId
    )
    this.houseKeeper = new MapDBStateStoreHouseKeeper(namingFactory)

  }

  override def stateStoreId: StateStoreId = stateStoreIdValue

  override def close(): Unit = {
    logInfo("Closing State Store Provider")
    db.close()
  }

  override def getStore(version: Long): StateStore = {
    logInfo(s"Getting the store for ${version}")
    if (version > 0 && lastCommittedVersion == NoCommittedVersionFlag) {
      val lastSnapshotVersion = if (version % this.stateStoreConf.minDeltasForSnapshot == 0) {
         version
      } else {
        (version / this.stateStoreConf.minDeltasForSnapshot) * this.stateStoreConf.minDeltasForSnapshot
      }

      if (lastSnapshotVersion > 0) {
        logInfo(s"Restoring for snapshot=${lastSnapshotVersion}")
      } else {
        logInfo(s"Snapshot not found, restoring from delta version ${lastSnapshotVersion}")
      }
      val restoredEntries = MapDBStateStoreRestorer(namingFactory, lastSnapshotVersion, version)
        .restoreFromSnapshot()
        .applyUpdatesAndDeletes()
        .getAllEntriesMap
      restoredEntries.getEntries.asScala.foreach(entry => println(entry))
      mapWithAllEntries.putAll(restoredEntries)
      logInfo(s"State restored correctly! Got ${mapWithAllEntries.size()} entries")
    }

    lastCommittedVersion = version
    new MapDBStateStore(previousVersion = version, mapAllEntriesDb = db,
      mapWithAllEntries = mapWithAllEntries,
      namingFactory = namingFactory,
      performLocalSnapshot = (version + 1 % this.stateStoreConf.minDeltasForSnapshot == 0),
      id = stateStoreIdValue,
      keySchema = this.keySchema, valueSchema = this.valueSchema)
  }

  override def doMaintenance(): Unit = {
    logInfo("Doing the maintenance for the store")
    if (lastCommittedVersion % this.stateStoreConf.minDeltasForSnapshot == 0) {
      houseKeeper.doCheckpointing(lastCommittedVersion)
    }
    houseKeeper.deleteTooOldVersions(minVersionsToRetain = stateStoreConf.minVersionsToRetain,
      lastCommittedVersion = lastCommittedVersion)
  }
}

object MapDBStateStoreProvider {
  val NoCommittedVersionFlag = -1L
}