/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.stats.distributions.{Multinomial => BrzMultinomial}
import org.apache.spark.ml.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.ml.linalg.Vectors

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class Magic(label: Double, str: String)

class StreamingNaiveBayesSuite extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  def createTestData() = {
    import spark.implicits._
    val input = MemoryStream[Magic]
    val indexer = new StringIndexerModel("meeps",
      Array("hi", "holden", "bye", "pandas"))
      .setInputCol("str")
      .setOutputCol("strIdx")
    val indexed = indexer.transform(input.toDS())
    val assembler = new VectorAssembler().setInputCols(Array("strIdx")).setOutputCol("features")
    val assembled = assembler.transform(indexed)
    val selected = assembled
      .select(col("label").cast(DoubleType), col("features"))
    val labelPoints = selected.map{
      case Row(label: Double, features: org.apache.spark.ml.linalg.Vector) =>
        org.apache.spark.ml.feature.LabeledPoint(label,
          org.apache.spark.ml.linalg.Vectors.dense(features.toArray)
        )
    }
    val inputData = List(
      Magic(0, "hi"), Magic(1, "holden"), Magic(0, "bye"), Magic(1, "pandas"))
    input.addData(inputData)
    (input, labelPoints)
  }
  test("test the streaming naive bayes using a sink") {
    val (input, labeledPoints) = createTestData()
    val query = labeledPoints.writeStream
      .queryName("testCustomSinkBasic")
      .format("com.highperformancespark.examples.structuredstreaming.StreamingNaiveBayesSinkprovider")
      .start()
    assert(query.isActive === true)
    query.processAllAvailable()

    assert(SimpleStreamingNaiveBayes.model.hasModel === true)
    query.stop()
    /*
    assert(query.exception === None)

    */
  }

  test("test streaming naive bayes using evil train") {
    val (input, labeledPoints) = createTestData()
    val sb = new StreamingNaiveBayes()
    assert(sb.hasModel === false)
    val query = sb.evilTrain(labeledPoints.toDF)
    assert(query.isActive === true)
    query.processAllAvailable()
    assert(query.exception === None)
    assert(sb.hasModel === true)
    query.stop()
  }

  def compareBatchAndStreaming(
      batch: NaiveBayesModel,
      streaming: StreamingNaiveBayesModel,
      validationData: DataFrame): Unit = {
    import spark.implicits._
    assert(batch.pi === streaming.pi)
    assert(batch.theta === streaming.theta)

    val batchPredictions = batch
      .transform(validationData)
      .select("rawPrediction", "probability", "prediction")
      .as[(Vector, Vector, Double)]
      .collect()
    val streamingPredictions = streaming
      .transform(validationData)
      .select("rawPrediction", "probability", "prediction")
      .as[(Vector, Vector, Double)]
      .collect()
    batchPredictions.zip(streamingPredictions).foreach {
      case ((batchRaw, batchProb, batchPred), (streamRaw, streamProb, streamPred)) =>
        assert(batchRaw === streamRaw)
        assert(batchProb === streamProb)
        assert(batchPred === streamPred)
    }
  }

  test("compare streaming intermediate results with batch") {
    import spark.implicits._
    val batches = StreamingNaiveBayesSuite.generateBatches(4, 3, 4, 10, 42)
    val inputStream = MemoryStream[LabeledPoint]
    val ds = inputStream.toDS()
    val sb = new StreamingNaiveBayes()
    val query = sb.evilTrain(ds.toDF())
    val streamingModels = batches.map { batch =>
      inputStream.addData(batch)
      query.processAllAvailable()
      sb.getModel
    }
    val batchData = new ArrayBuffer[LabeledPoint]
    val batchModels = batches.map { batch =>
      batchData ++= batch
      val df = batchData.toDF()
      val batchEstimator = new NaiveBayes()
      batchEstimator.fit(df)
    }
    batchModels.zip(streamingModels).foreach { case (batch, streaming) =>
      compareBatchAndStreaming(batch, streaming, batches.last.toDF())
    }
    query.stop()

  }
}

object StreamingNaiveBayesSuite {

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  private def generateRandomProbs(n: Int): Array[Double] = {
    var sum = 0.0
    val probs = Array.tabulate(n) { i =>
      val r = math.random
      sum += r
      r
    }
    probs.indices.foreach {i => probs(i) /= sum}
    probs
  }

  def generateBatches(
      numBatches: Int,
      numClasses: Int,
      numFeatures: Int,
      numPointsPerBatch: Int,
      seed: Int,
      sample: Int = 10): Seq[Seq[LabeledPoint]] = {
    val pi = generateRandomProbs(numClasses)
    val theta = Array.fill(numClasses)(generateRandomProbs(numFeatures))
    generateNaiveBayesInput(pi, theta, numPointsPerBatch, numBatches, seed, sample)
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
      pi: Array[Double],            // 1XC
      theta: Array[Array[Double]],  // CXD
      nPoints: Int,
      numBatches: Int,
      seed: Int,
      sample: Int = 10): Seq[Seq[LabeledPoint]] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.pow(math.E, _))
    val _theta = theta.map(row => row.map(math.pow(math.E, _)))

    for (j <- 0 until numBatches) yield {
      for (i <- 0 until nPoints) yield {
        val y = calcLabel(rnd.nextDouble(), _pi)
        val xi = {
          val mult = BrzMultinomial(BDV(_theta(y)))
          val emptyMap = (0 until D).map(x => (x, 0.0)).toMap
          val counts = emptyMap ++ mult.sample(sample).groupBy(x => x).map {
            case (index, reps) => (index, reps.size.toDouble)
          }
          counts.toArray.sortBy(_._1).map(_._2)
        }

        LabeledPoint(y, Vectors.dense(xi))
      }
    }
  }
}
