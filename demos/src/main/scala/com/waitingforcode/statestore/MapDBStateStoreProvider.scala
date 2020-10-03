package com.waitingforcode.statestore

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProvider}
import org.apache.spark.sql.types.StructType
import org.mapdb.{DBMaker, Serializer}

class MapDBStateStoreProvider extends StateStoreProvider {

  private var keySchema: StructType = _
  private var valueSchema: StructType = _
  private var checkpointStorePath: String = _
  private var localStorePath: String = _
  private var localSnapshotPath: String = _
  private var stateStoreIdValue: StateStoreId = _
  private var stateStoreConf: StateStoreConf = _
  private var lastCommittedVersion = -1L
  private var houseKeeper: MapDBStateStoreHouseKeeper = _

  private lazy val db = DBMaker
    .fileDB(s"${localStorePath}/state-${stateStoreIdValue.operatorId}-${stateStoreIdValue.partitionId}.db")
    .fileMmapEnableIfSupported()
    .make()

  // TODO: the questions I have regarding this db:
  //       - if the fileDB will contain a transaction log or only the most recent values ?
  private var mapWithAllEntries =
    db.hashMap(s"state-all-entries", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen()


  override def init(stateStoreId: StateStoreId, keySchema: StructType, valueSchema: StructType,
                    keyIndexOrdinal: Option[Int], storeConfs: StateStoreConf,
                    hadoopConf: Configuration): Unit = {
    println(s"Initializing the state store for ${stateStoreId}")
    this.keySchema = keySchema
    this.valueSchema = valueSchema
    this.stateStoreIdValue = stateStoreId
    this.stateStoreConf = storeConfs
    this.checkpointStorePath = storeConfs.confs("spark.sql.streaming.stateStore.mapdb.checkpointPath")
    this.localStorePath = storeConfs.confs("spark.sql.streaming.stateStore.mapdb.localPath")
    this.localSnapshotPath = s"${localStorePath}/snapshots/"
    if (!(new File(localStorePath).exists())) {
      new File(localStorePath).mkdirs()
    }
    this.houseKeeper = new MapDBStateStoreHouseKeeper(checkpointStorePath, localSnapshotPath)

  }

  override def stateStoreId: StateStoreId = stateStoreIdValue

  override def close(): Unit = {
    println("Closing State Store Provider")
    db.close()
  }

  override def getStore(version: Long): StateStore = {
    println(s"Getting the store for ${version}")
    if (version > 0) {
      val lastSnapshotVersion = if (version % this.stateStoreConf.minDeltasForSnapshot == 0) {
         version
      } else {
        (version / this.stateStoreConf.minDeltasForSnapshot) * this.stateStoreConf.minDeltasForSnapshot
      }
      println(s"Resting from snapshot=${lastSnapshotVersion}")
      mapWithAllEntries = MapDBStateStoreRestorer(checkpointStorePath, lastSnapshotVersion, version)
        .restoreFromSnapshot()
        .applyUpdatesAndDeletes()
        .getAllEntriesMap
      println("State restored correctly!")
    }

    lastCommittedVersion = version
    new MapDBStateStore(previousVersion = version, db = db,
      mapWithAllEntries = mapWithAllEntries,
      checkpointStorePath = this.checkpointStorePath,
      localSnapshotPath = this.localSnapshotPath,
      id = stateStoreIdValue,
      keySchema = this.keySchema, valueSchema = this.valueSchema)
  }

  override def doMaintenance(): Unit = {
    println("Doing the maintenance for the store")
    if (lastCommittedVersion % this.stateStoreConf.minDeltasForSnapshot == 0) {
      houseKeeper.doCheckpointing(lastCommittedVersion)
    }
    // TODO: handle deletes for minBatchesToRetain
  }
}
