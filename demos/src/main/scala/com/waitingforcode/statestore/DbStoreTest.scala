package com.waitingforcode.statestore

import java.io.File

import org.mapdb.{DBMaker, Serializer}

import scala.collection.JavaConverters.{asScalaSetConverter, iterableAsScalaIterableConverter}

object DbStoreTest extends App {

  val mapDir = "/tmp/data+ai/test/mapdb"
  //FileUtils.deleteDirectory(new File(mapDir))
  new File(mapDir).mkdirs()
  val db = DBMaker
    .fileDB(s"${mapDir}/test.db")
    .fileMmapEnableIfSupported()
    .transactionEnable()
    .make()

  val test1Map = db.hashMap("test1", Serializer.STRING, Serializer.STRING).createOrOpen()
  test1Map.put("a", "A")
  test1Map.put("b", "B")

  db.commit()

  println(s"Got test1Map=${test1Map.getEntries}")

  val test1MapReread = db.hashMap("test1", Serializer.STRING, Serializer.STRING).createOrOpen()
  test1MapReread.put("c", "C")
  println(s"Got test1MapReread=${test1MapReread.getEntries}")
  db.rollback()
  println(s"Got test1MapReread=${test1MapReread.getEntries}")

  val newMapContainer = db.hashMap("container", Serializer.STRING, Serializer.STRING).createOrOpen()
  test1MapReread.getEntries.asScala.foreach(entry => {
    newMapContainer.put(entry.getKey, entry.getValue)
    test1MapReread.remove(entry.getKey)
  })
  println(s"Got test1MapReread=${test1MapReread.getEntries}")
  println(s"Got newMapContainer=${newMapContainer.getEntries}")


  newMapContainer.getStores.distinct.foreach(store => {
    println(s"Got store=${store}")
    store.getAllFiles.asScala.foreach(deltaFile => {
      println(s"Got delta file=${deltaFile}")
    })
  })
}
