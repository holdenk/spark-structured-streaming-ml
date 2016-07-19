package com.highperformancespark.examples.structuredstreaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.FunSuite
import org.apache.log4j.{Level, Logger}

case class TestRow(features: Vector)

class StreamingKMeansSuite extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    Logger.getLogger("org").setLevel(Level.OFF)
  }

  test("compare intermediate batch and streaming") {
    import spark.implicits._
    val k = 2
    val dim = 5
    val clusterSpread = 0.1
    // TODO: this test is very flaky. The centers do not converge for some (most?) random seeds
    val (batches, trueCenters) =
      StreamingKMeansSuite.generateBatches(100, 80, k, dim, clusterSpread, 5)
    val inputStream = MemoryStream[TestRow]
    val ds = inputStream.toDS()
    val skm = new StreamingKMeans().setK(k).setRandomCenters(dim, 0.01)
    val query = skm.evilTrain(ds.toDF())
    val streamingModels = batches.map { batch =>
      inputStream.addData(batch)
      query.processAllAvailable()
      skm.getModel
    }
    // TODO: use spark's testing suite
    streamingModels.last.centers.zip(trueCenters).foreach { case (center, trueCenter) =>
      assert(center.toArray.zip(trueCenter.toArray).forall(x => math.abs(x._1 - x._2) < 0.1))
    }
    query.stop()
  }

  def compareBatchAndStreaming(
      batchModel: KMeansModel,
      streamingModel: StreamingKMeansModel,
      validationData: DataFrame): Unit = {
    assert(batchModel.clusterCenters === streamingModel.centers)
    // TODO: implement prediction comparison
  }

}

object StreamingKMeansSuite {

  def generateBatches(
      numPoints: Int,
      numBatches: Int,
      k: Int,
      d: Int,
      r: Double,
      seed: Int,
      initCenters: Array[Vector] = null): (IndexedSeq[IndexedSeq[TestRow]], Array[Vector]) = {
    val rand = scala.util.Random
    rand.setSeed(seed)
    val centers = initCenters match {
      case null => Array.fill(k)(Vectors.dense(Array.fill(d)(rand.nextGaussian())))
      case _ => initCenters
    }
    val data = (0 until numBatches).map { i =>
      (0 until numPoints).map { idx =>
        val center = centers(idx % k)
        val vec = Vectors.dense(Array.tabulate(d)(x => center(x) + rand.nextGaussian() * r))
        TestRow(vec)
      }
    }
    (data, centers)
  }
}
