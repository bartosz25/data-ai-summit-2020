package com.waitingforcode.statestore

import java.io.File

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
    new File(s"${mapDBTestDirectory}/test1").mkdirs()
    new File(s"${mapDBTestDirectory}/test2").mkdirs()
    new File(s"${mapDBTestDirectory}/test3").mkdirs()
    val dbTest1 = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test1/snapshot-5")
      .fileMmapEnableIfSupported()
      .make()
    dbTest1.hashMap("state-all-entries", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen().putAll(Map(
      "a".getBytes -> "1".getBytes,
      "b".getBytes -> "2".getBytes).asJava)
    dbTest1.close()

    val snapshotNumber = 5
    val dbTest2Snapshot = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test2/snapshot-${snapshotNumber}")
      .fileMmapEnableIfSupported()
      .make()
    dbTest2Snapshot.hashMap("state-all-entries", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen().putAll(Map(
      "a".getBytes -> "1".getBytes,
      "b".getBytes -> "2".getBytes).asJava)
    dbTest2Snapshot.close()
    (1 until 5).foreach(number => {
      val deltaNumber = snapshotNumber + number
      println(s"Writing ${deltaNumber}")
      val dbTestDelta = DBMaker
        .fileDB(s"${mapDBTestDirectory}/test2/delta-${deltaNumber}-update")
        .fileMmapEnableIfSupported()
        .make()
      dbTestDelta.hashMap("state-all-entries", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
        .create().putAll(Map(
        s"${deltaNumber}".getBytes -> s"${deltaNumber}".getBytes,
        "a".getBytes -> s"${deltaNumber}".getBytes,
        "b".getBytes -> s"${deltaNumber}".getBytes,
        "c".getBytes -> s"${deltaNumber}".getBytes).asJava)
      dbTestDelta.close()
    })
    val dbTestDeltaDelete = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test2/delta-9-delete")
      .fileMmapEnableIfSupported()
      .make()
    dbTestDeltaDelete.hashSet("state-all-entries", Serializer.BYTE_ARRAY)
      .createOrOpen().addAll(Seq("b".getBytes).asJavaCollection)
    dbTestDeltaDelete.close()

    println("Setup test3")
    (1 until 5).foreach(deltaNumber => {
      println(s"Writing ${deltaNumber}")
      val dbTest3Delta = DBMaker
        .fileDB(s"${mapDBTestDirectory}/test3/delta-${deltaNumber}-update")
        .fileMmapEnableIfSupported()
        .make()
      dbTest3Delta.hashMap("state-all-entries", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
        .create().putAll(Map(
        s"${deltaNumber}".getBytes -> s"${deltaNumber}".getBytes,
        "a".getBytes -> s"${deltaNumber}".getBytes,
        "b".getBytes -> s"${deltaNumber}".getBytes,
        "c".getBytes -> s"${deltaNumber}".getBytes).asJava)
      dbTest3Delta.close()
    })
    val dbTest3DeltaDelete = DBMaker
      .fileDB(s"${mapDBTestDirectory}/test3/delta-3-delete")
      .fileMmapEnableIfSupported()
      .make()
    dbTest3DeltaDelete.hashSet("state-all-entries", Serializer.BYTE_ARRAY)
      .createOrOpen().addAll(Seq("b".getBytes).asJavaCollection)
    dbTest3DeltaDelete.close()
  }

  after {
    FileUtils.deleteDirectory(new File(mapDBTestDirectory))
  }

  it should "restore the database only from the snapshot version" in {
    val allEntries = MapDBStateStoreRestorer(s"${mapDBTestDirectory}/test1/", 5, 5)
      .restoreFromSnapshot()
      .applyUpdatesAndDeletes()
      .getAllEntriesMap

    val restoredEntries =
      allEntries.getEntries.asScala.map(entry => (new String(entry.getKey), new String(entry.getValue)))

    restoredEntries should have size 2
    restoredEntries should contain allOf(("a", "1"), ("b", "2"))
  }

  it should "restore the database from snapshot and delta files" in {
    val allEntries = MapDBStateStoreRestorer(s"${mapDBTestDirectory}/test2/", 5, 9)
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
    val allEntries = MapDBStateStoreRestorer(s"${mapDBTestDirectory}/test3/", 0, 4)
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
