package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mapdb.{DB, DBMaker, Serializer}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.asScalaSetConverter

class MapDBStateStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  private val testTemporaryDir = "/tmp/data+ai/test/mapdbstatestore"
  private val keySchema = StructType(Array(
    StructField("id", StringType, nullable = false)
  ))
  private val valueSchema = StructType(Array(
    StructField("letter", StringType, nullable = false)
  ))

  before {
    new File(testTemporaryDir).mkdirs()
  }
  after {
    FileUtils.deleteDirectory(new File(testTemporaryDir))
  }

  behavior of "MapDB-backed state store"

  it should "correctly add and get the added element" in {
    val dbStore = testedMapDbStateStore(1)
    val keyToAdd = unsafeRow(keySchema, "1")
    val valueToAdd = unsafeRow(valueSchema, "A")

    dbStore.put(keyToAdd, valueToAdd)
    val addedElement = dbStore.get(keyToAdd)

    addedElement shouldEqual valueToAdd
    val allElements = dbStore.iterator().toSeq
    allElements should have size 1
    allElements(0).key shouldEqual keyToAdd
    allElements(0).value shouldEqual valueToAdd
  }

  it should "correctly remove already present and new element" in {
    val dbStore = testedMapDbStateStore(2, Seq(
      ("1", "a"), ("2", "b")
    ))
    val keyToRemove = unsafeRow(keySchema, "3")
    val valueToAdd = unsafeRow(valueSchema, "A")

    dbStore.put(keyToRemove, valueToAdd)
    dbStore.remove(keyToRemove)
    dbStore.remove(unsafeRow(keySchema, "2"))

    val allEntries = dbStore.iterator().toSeq
    allEntries should have size 1
    allEntries(0).key shouldEqual unsafeRow(keySchema, "1")
    allEntries(0).value shouldEqual unsafeRow(valueSchema, "a")
  }

  it should "correctly overwrite already present element" in {
    val dbStore = testedMapDbStateStore(3, Seq(
      ("1", "a"), ("2", "b")
    ))
    val keyToAdd = unsafeRow(keySchema, "1")
    val valueToAdd = unsafeRow(valueSchema, "A")

    dbStore.put(keyToAdd, valueToAdd)

    val allEntries = dbStore.iterator().toSeq.map(entry => (entry.key, entry.value))
    allEntries should have size 2
    allEntries should contain allOf((keyToAdd, valueToAdd),
      (unsafeRow(keySchema, "2"), unsafeRow(valueSchema, "b")))
  }

  it should "correctly add, remove and update the elements and write them to the delta and local snapshot" in {
    val dbStore = testedMapDbStateStore(4, Seq(
      ("1", "a"), ("2", "b")
    ), performSnapshot = true)
    val keyToAdd = unsafeRow(keySchema, "3")
    val valueToAdd = unsafeRow(valueSchema, "C")
    val keyToUpdate = unsafeRow(keySchema, "1")
    val valueToUpdate = unsafeRow(valueSchema, "A")
    val keyToRemove = unsafeRow(keySchema, "2")

    dbStore.put(keyToAdd, valueToAdd)
    dbStore.remove(keyToRemove)
    dbStore.put(keyToUpdate, valueToUpdate)
    dbStore.commit()

    val updatesDb = testedDb("/tmp/data+ai/test/mapdbstatestore/test4/checkpoint/updates-1-0.db")
    val updatesSavedMap = updatesDb
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val updatedEntries = updatesSavedMap.getEntries.asScala.map(entry => {
      (new String(entry.getKey), new String(entry.getValue))
    }).toSeq
    updatedEntries should have size 2
    updatedEntries should contain allOf(("3", "C"), ("1", "A"))

    val deletesDb = testedDb("/tmp/data+ai/test/mapdbstatestore/test4/checkpoint/deletes-1-0.db")
    val deletesSet = deletesDb
      .hashSet(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val deletedEntries = deletesSet.asScala.map(entry => new String(entry)).toSeq
    deletedEntries should have size 1
    deletedEntries(0) shouldEqual "2"

    val localSnapshotDb = testedDb("/tmp/data+ai/test/mapdbstatestore/test4/snapshot/1/snapshot-1-0.db")
    val localSnapshotMap = localSnapshotDb
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val snapshotEntries = localSnapshotMap.getEntries.asScala.map(entry => {
      (new String(entry.getKey), new String(entry.getValue))
    })
    snapshotEntries should have size 2
    snapshotEntries should contain allOf(("3", "C"), ("1", "A"))
  }

  it should "abort the state store changes and not write the local files" in {

  }

  private def unsafeRow(schema: StructType, value: String): UnsafeRow = {
    val bytes = value.getBytes
    val unsafeRowFromBytes = new UnsafeRow(schema.fields.length)
    unsafeRowFromBytes.pointTo(bytes, bytes.length)
    unsafeRowFromBytes
  }

  private def testedMapDbStateStore(testNumber: Int,
                                    defaultEntries: Seq[(String, String)] = Seq.empty,
                                    performSnapshot: Boolean = false) = {
    val testDir = s"${testTemporaryDir}/test${testNumber}"
    new File(testDir).mkdirs()
    new File(s"${testDir}/checkpoint/1").mkdirs()
    new File(s"${testDir}/snapshot/1").mkdirs()
    new File(s"${testDir}/local-store/1").mkdirs()
    val db = testedDb(s"${testDir}/local-store/state-1-0.db")
    val mapWithAllEntries = db
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    defaultEntries.foreach {
      case (key, value) => mapWithAllEntries.put(
        unsafeRow(keySchema, key).getBytes, unsafeRow(valueSchema, value).getBytes
      )
    }
    new MapDBStateStore(
      previousVersion = 0,
      id = StateStoreId(
        checkpointRootLocation = "", operatorId = 1L, partitionId = 0
      ),
      checkpointStorePath = s"${testDir}/checkpoint",
      localSnapshotPath = s"${testDir}/snapshot",
      performLocalSnapshot = performSnapshot,
      localStorePath = s"${testDir}/local-store",
      keySchema = keySchema,
      valueSchema = valueSchema,
      mapAllEntriesDb = db,
      mapWithAllEntries = mapWithAllEntries
    )
  }

  def testedDb(testFile: String): DB = {
    DBMaker
      .fileDB(testFile)
      .fileMmapEnableIfSupported()
      .make()
  }

}
