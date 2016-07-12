/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.stats.distributions.{Multinomial => BrzMultinomial}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.linalg.Vectors

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors

import scala.util.Random

case class Magic(label: Double, str: String)

class StreamingNaiveBayesSuite extends FunSuite with DataFrameSuiteBase {

  @transient var dataset: Dataset[_] = _
  @transient var streamingDataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    import spark.implicits._
    super.beforeAll()

    val pi = Array(0.5, 0.1, 0.4).map(math.log)
    val theta = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))

    val data = StreamingNaiveBayesSuite.generateNaiveBayesInput(pi, theta, 100, 42)
    dataset = spark.createDataFrame(data)

    val input = MemoryStream[LabeledPoint]
    streamingDataset = input.toDS()
    input.addData(data)
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

  test("test query based naive bayes") {
    import spark.implicits._
    val (input, labeledPoints) = createTestData()
    val QueryBasedStreamingNaiveBayes = new QueryBasedStreamingNaiveBayes()
    val (model, query) = QueryBasedStreamingNaiveBayes.train(labeledPoints.toDF.as[LabeledPoint])
    assert(query.isActive === true)
    query.processAllAvailable()
    // Console sink example
    labeledPoints.writeStream.format("console").start().processAllAvailable()
    assert(
      (List(List(1), List(0)), List(LabelCount(0.0,2), LabelCount(1.0,2)))
        === model.counts(Vectors.dense(Array(0.0))))
  }

  test("test the streaming naive bayes using a sink") {
    val (input, labeledPoints) = createTestData()
    val query = labeledPoints.writeStream
      .queryName("testCustomSinkBasic")
      .format("com.highperformancespark.examples.structuredstreaming.StreamingNaiveBayesSinkprovider")
      .start()
    assert(query.isActive === true)
    query.processAllAvailable()
//    query.stop()

    /*
    assert(query.exception === None)
    assert(SimpleStreamingNaiveBayes.model.hasModel === true)
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

  test("streaming matches batch") {
    val sb = new StreamingNaiveBayes()
    val query = sb.evilTrain(streamingDataset.toDF)
    query.processAllAvailable()
    val streamingModel = sb.getModel

    val batchNB = new NaiveBayes()
    val batchModel = batchNB.fit(dataset)
    assert(batchModel.pi === streamingModel.pi)
    assert(batchModel.theta === streamingModel.theta)
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

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
      pi: Array[Double],            // 1XC
      theta: Array[Array[Double]],  // CXD
      nPoints: Int,
      seed: Int,
      sample: Int = 10): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.pow(math.E, _))
    val _theta = theta.map(row => row.map(math.pow(math.E, _)))

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
