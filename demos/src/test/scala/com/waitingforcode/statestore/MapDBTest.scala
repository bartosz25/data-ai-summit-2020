package com.waitingforcode.statestore

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.mapdb.{DBMaker, Serializer}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter}
import scala.util.Try

class MapDBTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  behavior of "MapDB"

  it should "create a database backed by a temporary file" in {
    val database = DBMaker.tempFileDB().make()
    val lettersSet = database.hashSet("letters", Serializer.STRING).create()

    lettersSet.add("a")
    lettersSet.add("b")
    lettersSet.add("c")
    lettersSet.add("a")

    lettersSet.size() shouldEqual 3
    lettersSet.iterator().asScala.toSeq should contain allOf("a", "b", "c")
  }

  it should "create file-backed map and restore it after closing" in {
    val testDir = "/tmp/mapdb-test"
    FileUtils.deleteDirectory(new File(testDir))
    new File(testDir).mkdirs()
    val databaseFile = s"${testDir}/file1.db"
    val fileBasedDatabase = DBMaker.fileDB(databaseFile).make()
    val lettersSet = fileBasedDatabase.hashSet("letters", Serializer.STRING).create()

    Seq("a", "b", "c", "a").foreach(letter => lettersSet.add(letter))
    fileBasedDatabase.close()
    val restoredDatabase = DBMaker.fileDB(databaseFile).make()
    val restoredLettersSet = restoredDatabase.hashSet("letters", Serializer.STRING).open()
    Seq("d", "e", "f", "e").foreach(letter => restoredLettersSet.add(letter))

    restoredLettersSet.size() shouldEqual 6
    restoredLettersSet.iterator().asScala.toSeq should contain allOf("a", "b", "c", "d", "e", "f")
    val fileNames = FileUtils.listFiles(new File(testDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.map(file => {
      file.getName
    }).toSeq
    fileNames should have size 1
    fileNames(0) shouldEqual "file1.db"
  }

  it should "create and restore 2 HashMaps" in {
    val testDir = "/tmp/mapdb-test-2"
    FileUtils.deleteDirectory(new File(testDir))
    new File(testDir).mkdirs()
    (0 to 2).foreach(runNumber => {
      println(s"Executing ${runNumber}")
      val databaseFile = s"${testDir}/file2.db"
      val fileBasedDatabase = DBMaker.fileDB(databaseFile).make()
      val stringsMap = fileBasedDatabase.hashMap("strings", Serializer.STRING, Serializer.STRING).createOrOpen()
      val intsMap = fileBasedDatabase.hashMap("ints", Serializer.INTEGER, Serializer.INTEGER).createOrOpen()
      if (runNumber == 0) {
        println(" Adding new values")
        stringsMap.put("a", "A")
        intsMap.put(1, 10)
        intsMap.put(2, 20)
      }

      stringsMap.size() shouldEqual 1
      stringsMap.get("a") shouldEqual "A"
      intsMap.size() shouldEqual 2
      intsMap.get(1) shouldEqual 10
      intsMap.get(2) shouldEqual 20

      stringsMap.close()
      intsMap.close()
      fileBasedDatabase.close()
    })
  }

  it should "create a transactional database" in {
    val testDir = "/tmp/mapdb-test-3"
    FileUtils.deleteDirectory(new File(testDir))
    new File(testDir).mkdirs()
    val transactionalDatabase = DBMaker.fileDB(s"${testDir}/transactional-database-1.db").transactionEnable().make()
    val lettersSet = transactionalDatabase.hashSet("letters", Serializer.STRING).create()

    // Let's add first some letters and commit the transaction
    lettersSet.add("a")
    lettersSet.add("b")
    transactionalDatabase.commit()

    lettersSet.iterator().asScala.toSeq should contain allOf("a", "b")

    // And now, add new letters but rollback the transaction
    lettersSet.add("c")
    // No commit, so no visibility for the pending changes
    lettersSet.iterator().asScala.toSeq should contain allOf("a", "b")
    // Rollback now
    transactionalDatabase.rollback()
    lettersSet.iterator().asScala.toSeq should contain allOf("a", "b")
  }

  it should "not restore not transactional store and restore the transactional one" in {
    val testDir = "/tmp/mapdb-test-4"
    FileUtils.deleteDirectory(new File(testDir))
    new File(testDir).mkdirs()

    val transactionalDatabase = DBMaker.fileDB(s"${testDir}/transactional-database-1.db").transactionEnable().make()
    val lettersSet = transactionalDatabase.hashSet("letters", Serializer.STRING).create()
    lettersSet.add("z")
    transactionalDatabase.commit()
    Try {
      Seq("a", "b", "c").foreach(letter => {
        lettersSet.add(letter)
        if (letter == "b") {
          throw new RuntimeException("An unexpected error")
        }
      })
    }

    val nonTransactionalDatabase = DBMaker.fileDB(s"${testDir}/nontransactional-database-1.db").make()
    val nonTransactionalLettersSet = nonTransactionalDatabase.hashSet("letters", Serializer.STRING).create()
    nonTransactionalLettersSet.add("z")
    Try {
      Seq("a", "b", "c").foreach(letter => {
        nonTransactionalLettersSet.add(letter)
        if (letter == "b") {
          throw new RuntimeException("An unexpected error")
        }
      })
    }

    // Do not call .close() on these DBs! If you app crashes, you won't have a chance to do so
    // Call `RestoreTransactionalAndNonTransactionalDatabases` later to simulate the application
    // restart using the same files
  }

}
