package com.waitingforcode.statestore

import org.mapdb.{DBMaker, Serializer}

import scala.collection.JavaConverters.asScalaSetConverter
import scala.util.Try


object RestoreTransactionalAndNonTransactionalDatabases extends App {
  // Run me after 'it should "not restore not transactional store and restore the transactional one"'
  // Not succeeded to execute it automatically after the test (has to be another JVM)
  val testDir = "/tmp/mapdb-test-4"
  // Restore the transactional db
  val restoredTransactionalDatabase = DBMaker.fileDB(s"${testDir}/transactional-database-1.db").transactionEnable().make()
  val restoredLettersSet = restoredTransactionalDatabase.hashSet("letters", Serializer.STRING).open()
  val restoredLettersTransactional = restoredLettersSet.asScala.toSeq

  assert(restoredLettersTransactional.length == 1 && restoredLettersTransactional(0) == "z",
    "Unexpected items in the restored set")

  // Restore the non transactional, it will fail
  val failedRestore = Try {
    DBMaker.fileDB(s"${testDir}/nontransactional-database-1.db").make()
  }.failed
  assert(failedRestore.get.getClass.toString == "class org.mapdb.DBException$DataCorruption",
    "DataCorruption was expected")
}