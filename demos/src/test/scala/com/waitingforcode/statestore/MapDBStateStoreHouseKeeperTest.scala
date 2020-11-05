package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable

class MapDBStateStoreHouseKeeperTest extends AnyFlatSpec with Matchers {

  "housekeeper" should "keep only 3 last versions" in {
    FileUtils.deleteDirectory(new File("/tmp/test-housekeeping/"))
    val namingFactory = MapDBStateStoreNamingFactory(
      checkpointStorePath = "/tmp/test-housekeeping/checkpoint",
      localStorePath = "/tmp/test-housekeeping/local",
      operatorId = 1, partitionNumber = 100, stateStoreName = "default"
    )
    val expectedFiles = new mutable.ListBuffer[String]()
    (1 to 10).foreach(version => {
      val delete = new File(namingFactory.checkpointDeltaForDelete(version))
      val update = new File(namingFactory.checkpointDeltaForUpdate(version))
      val snapshot = new File(namingFactory.checkpointSnapshot(version))
      delete.createNewFile()
      update.createNewFile()
      snapshot.createNewFile()
      if (version < 4 || version > 6) {
        expectedFiles.append(delete.getAbsolutePath, update.getAbsolutePath, snapshot.getAbsolutePath)
      }
    })

    val minVersionsToRetain = 3
    val lastCommittedVersion = 10
    val houseKeeper = new MapDBStateStoreHouseKeeper(namingFactory)
    houseKeeper.deleteTooOldVersions(minVersionsToRetain, lastCommittedVersion)

    val filesAfterRemoval = FileUtils.listFiles(new File("/tmp/test-housekeeping/"),
      Array("db"), true).asScala.map(file => file.getAbsolutePath).toSeq
    filesAfterRemoval should have size expectedFiles.size
    expectedFiles should contain allElementsOf(filesAfterRemoval)
  }

}
