/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors

case class Magic(label: Double, str: String)

class StreamingNaiveBayesSuite extends FunSuite with DataFrameSuiteBase {

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
    assert(query.exception === None)
    assert(SimpleStreamingNaiveBayes.model.hasModel === true)
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
  }
}
