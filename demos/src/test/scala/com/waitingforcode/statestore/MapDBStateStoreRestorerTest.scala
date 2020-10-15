package com.waitingforcode.statestore

import java.io.File

import com.waitingforcode.statestore.MapDBStateStore.EntriesName
import org.apache.commons.io.FileUtils
import org.mapdb.{DBMaker, Serializer}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaSetConverter, mapAsJavaMapConverter}


class MapDBStateStoreRestorerTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val mapDBTestDirectory = "/tmp/data+ai/test/mapdb/restore"
  behavior of "MapDBStateRestorer"

  before {
    println("Setup test1")
    new File(s"${mapDBTestDirectory}/test1/checkpoint/5/").mkdirs()
    val dbTest1 = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test1/checkpoint/5/snapshot-1-0.db")
      .fileMmapEnableIfSupported()
      .make()
    dbTest1.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen().putAll(Map(
      "a".getBytes -> "1".getBytes,
      "b".getBytes -> "2".getBytes).asJava)
    dbTest1.close()

    println("Setup test2")
    new File(s"${mapDBTestDirectory}/test2/checkpoint/5").mkdirs()
    val snapshotNumber = 5
    val dbTest2Snapshot = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test2/checkpoint/5/snapshot-1-0.db")
      .fileMmapEnableIfSupported()
      .make()
    dbTest2Snapshot.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen().putAll(Map(
      "a".getBytes -> "1".getBytes,
      "b".getBytes -> "2".getBytes).asJava)
    dbTest2Snapshot.close()
    val namingFactory = MapDBStateStoreNamingFactory(s"${mapDBTestDirectory}/test2/checkpoint/",
      s"${mapDBTestDirectory}/test2/local/", 1L, 0)
    (1 until 5).foreach(number => {
      val deltaNumber = snapshotNumber + number
      new File(s"${mapDBTestDirectory}/test2/checkpoint/${deltaNumber}").mkdirs()
      println(s"Writing ${deltaNumber}")
      val dbTestDelta = DBMaker
        .fileDB(namingFactory.checkpointDeltaForUpdate(deltaNumber))
        .fileMmapEnableIfSupported()
        .make()
      dbTestDelta.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
        .create().putAll(Map(
        s"${deltaNumber}".getBytes -> s"${deltaNumber}".getBytes,
        "a".getBytes -> s"${deltaNumber}".getBytes,
        "b".getBytes -> s"${deltaNumber}".getBytes,
        "c".getBytes -> s"${deltaNumber}".getBytes).asJava)
      dbTestDelta.close()
    })
    val dbTestDeltaDelete = DBMaker
      .fileDB(namingFactory.checkpointDeltaForDelete(9))
      .fileMmapEnableIfSupported()
      .make()
    dbTestDeltaDelete.hashSet(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY)
      .createOrOpen().addAll(Seq("b".getBytes).asJavaCollection)
    dbTestDeltaDelete.close()

    println("Setup test3")
    new File(s"${mapDBTestDirectory}/test3").mkdirs()
    val namingFactoryTest3 = MapDBStateStoreNamingFactory(s"${mapDBTestDirectory}/test3/checkpoint/",
      s"${mapDBTestDirectory}/test3/local/", 1L, 0)
    (1 until 5).foreach(deltaNumber => {
      println(s"Writing ${deltaNumber}")
      new File(s"${mapDBTestDirectory}/test3/checkpoint/${deltaNumber}").mkdirs()
      val dbTest3Delta = DBMaker
        .fileDB(namingFactoryTest3.checkpointDeltaForUpdate(deltaNumber))
        .fileMmapEnableIfSupported()
        .make()
      dbTest3Delta.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
        .create().putAll(Map(
        s"${deltaNumber}".getBytes -> s"${deltaNumber}".getBytes,
        "a".getBytes -> s"${deltaNumber}".getBytes,
        "b".getBytes -> s"${deltaNumber}".getBytes,
        "c".getBytes -> s"${deltaNumber}".getBytes).asJava)
      dbTest3Delta.close()
    })
    val dbTest3DeltaDelete = DBMaker
      .fileDB(namingFactoryTest3.checkpointDeltaForDelete(3))
      .fileMmapEnableIfSupported()
      .make()
    dbTest3DeltaDelete.hashSet(EntriesName, Serializer.BYTE_ARRAY)
      .createOrOpen().addAll(Seq("b".getBytes).asJavaCollection)
    dbTest3DeltaDelete.close()
  }

  after {
    FileUtils.deleteDirectory(new File(mapDBTestDirectory))
  }

  it should "restore the database only from the snapshot version" in {
    val namingFactory = MapDBStateStoreNamingFactory(s"${mapDBTestDirectory}/test1/checkpoint",
      s"${mapDBTestDirectory}/test1/local", 1L, 0)
    val allEntries = MapDBStateStoreRestorer(namingFactory, 5, 5)
      .restoreFromSnapshot()
      .applyUpdatesAndDeletes()
      .getAllEntriesMap

    val restoredEntries =
      allEntries.getEntries.asScala.map(entry => (new String(entry.getKey), new String(entry.getValue)))

    restoredEntries should have size 2
    restoredEntries should contain allOf(("a", "1"), ("b", "2"))
  }

  it should "restore the database from snapshot and delta files" in {
    val namingFactory = MapDBStateStoreNamingFactory(s"${mapDBTestDirectory}/test2/checkpoint",
      s"${mapDBTestDirectory}/test2/local", 1L, 0)
    val allEntries = MapDBStateStoreRestorer(namingFactory, 5, 9)
      .restoreFromSnapshot()
      .applyUpdatesAndDeletes()
      .getAllEntriesMap

    val restoredEntries =
      allEntries.getEntries.asScala.map(entry => (new String(entry.getKey), new String(entry.getValue)))

    restoredEntries should have size 6
    restoredEntries should contain allOf(
      ("6", "6"), ("7", "7"), ("8", "8"), ("9", "9"),
      ("a", "9"), ("c", "9"))
  }

  it should "restore the database only from the delta files" in {
    val namingFactory = MapDBStateStoreNamingFactory(s"${mapDBTestDirectory}/test3/checkpoint",
      s"${mapDBTestDirectory}/test3/local", 1L, 0)
    val allEntries = MapDBStateStoreRestorer(namingFactory, 0, 4)
      .restoreFromSnapshot()
      .applyUpdatesAndDeletes()
      .getAllEntriesMap

    val restoredEntries =
      allEntries.getEntries.asScala.map(entry => (new String(entry.getKey), new String(entry.getValue)))

    restoredEntries should have size 7
    restoredEntries should contain allOf(
      ("1", "1"), ("2", "2"), ("3", "3"), ("4", "4"),
      ("a", "4"), ("b", "4"), ("c", "4"))
  }

}
