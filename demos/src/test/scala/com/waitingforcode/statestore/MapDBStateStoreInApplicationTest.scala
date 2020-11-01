package com.waitingforcode.statestore

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MapDBStateStoreInApplicationTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val mapDbLocalStore = "/tmp/data+ai/test/mapdb/test_in_spark"
  FileUtils.deleteDirectory(new File(mapDbLocalStore))
  SparkSession.clearActiveSession()
  val testSession: SparkSession = SparkSession.builder()
    .appName("Test MapDBStateStore in an app").master("local[*]")
    .config("spark.sql.shuffle.partitions", 12)
    .config("spark.sql.streaming.stateStore.mapdb.localPath", mapDbLocalStore)
    .config("spark.sql.streaming.stateStore.mapdb.checkpointPath", s"${mapDbLocalStore}/checkpoint")
    .config(SQLConf.STATE_STORE_PROVIDER_CLASS.key, classOf[MapDBStateStoreProvider].getCanonicalName)
    .getOrCreate()
  after {
    SparkSession.clearActiveSession()
  }

  "custom MapDB-state store" should "generate correct count results" in {
    import testSession.implicits._
    val inputStream = new MemoryStream[Int](1, testSession.sqlContext)
    inputStream.addData(Seq(1, 2, 3))

    val query = inputStream.toDF().agg("*" -> "count")
      .writeStream
      .outputMode("update")
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(value: Row): Unit = {
          InMemoryAggregationStore.counter.set(value.getLong(0))
        }
        override def close(errorOrNull: Throwable): Unit = {}
      })
    query.start().awaitTermination(15000L)

    InMemoryAggregationStore.counter.get() shouldEqual 3
  }

}

object InMemoryAggregationStore {
  val counter = new AtomicLong(0L)
}
