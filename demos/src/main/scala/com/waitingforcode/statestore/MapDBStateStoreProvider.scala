package com.waitingforcode.statestore

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.types.StructType
import org.mapdb.{DBMaker, Serializer}

class MapDBStateStoreProvider extends StateStoreProvider with Logging {

  private var keySchema: StructType = _
  private var valueSchema: StructType = _
  private var stateStoreIdValue: StateStoreId = _
  private var stateStoreConf: StateStoreConf = _
  private var lastCommittedVersion = -1L
  private var houseKeeper: MapDBStateStoreHouseKeeper = _
  private var namingFactory: MapDBStateStoreNamingFactory = _

  private lazy val db = DBMaker
    .fileDB(namingFactory.allEntriesFile)
    .fileMmapEnableIfSupported()
    .make()

  // TODO: the questions I have regarding this db:
  //       - if the fileDB will contain a transaction log or only the most recent values ?
  private var mapWithAllEntries =
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
    if (version > 0) {
      val lastSnapshotVersion = if (version % this.stateStoreConf.minDeltasForSnapshot == 0) {
         version
      } else {
        (version / this.stateStoreConf.minDeltasForSnapshot) * this.stateStoreConf.minDeltasForSnapshot
      }

      logInfo(s"Resting from snapshot=${lastSnapshotVersion}")
      mapWithAllEntries = MapDBStateStoreRestorer(namingFactory, lastSnapshotVersion, version)
        .restoreFromSnapshot()
        .applyUpdatesAndDeletes()
        .getAllEntriesMap
      logInfo("State restored correctly!")
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
