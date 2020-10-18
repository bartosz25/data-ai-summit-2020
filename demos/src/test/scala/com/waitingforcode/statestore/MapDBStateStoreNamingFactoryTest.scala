package com.waitingforcode.statestore

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MapDBStateStoreNamingFactoryTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  behavior of "MapDBStateStoreNamingFactory"

  private val namingFactoryToTest = MapDBStateStoreNamingFactory("/checkpoint", "/local", 100L, 2)

  it should "generate correct names for the local storage" in {
    namingFactoryToTest.localDeltaForUpdate(2L) shouldEqual "/local/2/updates-100-2.db"
    namingFactoryToTest.localDeltaForDelete(2L) shouldEqual "/local/2/deletes-100-2.db"
    namingFactoryToTest.localSnapshot(2L) shouldEqual "/local/2/snapshot-100-2.db"
    namingFactoryToTest.allEntriesFile shouldEqual "/local/all-entries-100-2.db"
  }

  it should "generate correct names for the checkpoint storage" in {
    namingFactoryToTest.checkpointDeltaForUpdate(2L) shouldEqual "/checkpoint/2/updates-100-2.db"
    namingFactoryToTest.checkpointDeltaForDelete(2L) shouldEqual "/checkpoint/2/deletes-100-2.db"
    namingFactoryToTest.checkpointSnapshot(2L) shouldEqual "/checkpoint/2/snapshot-100-2.db"
  }

}
