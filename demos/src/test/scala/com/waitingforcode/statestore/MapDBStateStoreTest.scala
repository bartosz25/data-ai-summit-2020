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

import scala.collection.JavaConverters.{asScalaSetConverter, collectionAsScalaIterableConverter}

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
    val keyToAdd3 = unsafeRow(keySchema, "3")
    val valueToAdd3 = unsafeRow(valueSchema, "C")

    dbStore.put(keyToAdd, valueToAdd)
    dbStore.put(keyToAdd3, valueToAdd3)

    val allEntries = dbStore.iterator().toSeq.map(entry => (entry.key, entry.value))
    allEntries should have size 3
    allEntries should contain allOf((keyToAdd, valueToAdd), (keyToAdd3, valueToAdd3),
      (unsafeRow(keySchema, "2"), unsafeRow(valueSchema, "b")))
  }

  it should "correctly add, remove and update the elements and write them to the delta and local snapshot" in {
    val testBaseDir = "/tmp/data+ai/test/mapdbstatestore/test4"
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

    val updatesDb = testedDb(s"${testBaseDir}/checkpoint/1/updates-main-1-0.db")
    val updatesSavedMap = updatesDb
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val updatedEntries = updatesSavedMap.getEntries.asScala.map(entry => {
      (new String(entry.getKey), new String(entry.getValue))
    }).toSeq
    updatedEntries should have size 2
    updatedEntries should contain allOf(("3", "C"), ("1", "A"))

    val deletesDb = testedDb(s"${testBaseDir}/checkpoint/1/deletes-main-1-0.db")
    val deletesSet = deletesDb
      .hashSet(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val deletedEntries = deletesSet.asScala.map(entry => new String(entry)).toSeq
    deletedEntries should have size 1
    deletedEntries(0) shouldEqual "2"

    val localSnapshotDb = testedDb(s"${testBaseDir}/local/1/snapshot-main-1-0.db")
    val localSnapshotMap = localSnapshotDb
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    val snapshotEntries = localSnapshotMap.getEntries.asScala.map(entry => {
      (new String(entry.getKey), new String(entry.getValue))
    })
    snapshotEntries should have size 2
    snapshotEntries should contain allOf(("3", "C"), ("1", "A"))
    dbStore.hasCommitted shouldBe true
    val localFilesAfterCommit = FileUtils.listFiles(new File(s"${testBaseDir}/local/1"), null, true)
      .asScala.map(file => file.getName).toSeq
    localFilesAfterCommit should have size 1
    localFilesAfterCommit(0) shouldEqual "snapshot-main-1-0.db"
  }

  it should "abort the state store changes and not write the local files" in {
    // Init some entries first
    new File("/tmp/data+ai/test/mapdbstatestore/test5/local/").mkdirs()
    val initStoreMap = testedDb("/tmp/data+ai/test/mapdbstatestore/test5/local/all-entries.db")
    val initValues = initStoreMap
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).create()
    initValues.put("1".getBytes, "A".getBytes)
    initStoreMap.commit()
    initValues.close()
    // Create tested state store now
    val dbStore = testedMapDbStateStore(5, Seq(
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
    dbStore.abort()

    dbStore.hasCommitted shouldBe false
    val abortedDb = testedDb("/tmp/data+ai/test/mapdbstatestore/test5/local/all-entries.db")
    val abortedMap = abortedDb.hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).open()
    abortedMap.size() shouldEqual   1
    abortedMap.get("1".getBytes) shouldEqual "A".getBytes
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
    val namingFactory =  MapDBStateStoreNamingFactory(s"${testDir}/checkpoint",
      s"${testDir}/local", 1L, 0, "main")
    new File(s"${testDir}/checkpoint").mkdirs()
    new File(s"${testDir}/local").mkdirs()
    val db = testedDb(namingFactory.allEntriesFile)
    val mapWithAllEntries = db
      .hashMap(MapDBStateStore.EntriesName, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY)
      .createOrOpen()
    defaultEntries.foreach {
      case (key, value) => mapWithAllEntries.put(
        unsafeRow(keySchema, key).getBytes, unsafeRow(valueSchema, value).getBytes
      )
    }
    new File(s"${testDir}/local/1").mkdirs()
    new MapDBStateStore(
      previousVersion = 0,
      id = StateStoreId(
        checkpointRootLocation = "", operatorId = 1L, partitionId = 0
      ),
      performLocalSnapshot = performSnapshot,
      namingFactory = namingFactory,
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
      .transactionEnable()
      .make()
  }

}
