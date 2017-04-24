/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import com.holdenkarau.spark.testing.DataFrameSuiteBase

import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.sql.{Dataset, DataFrame, Encoder, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream

class CustomSinkSuite extends FunSuite with DataFrameSuiteBase {

  test("really simple test of the custom sink") {
    import spark.implicits._
    val input = MemoryStream[String]
    val doubled = input.toDS().map(x => x + " " + x)
    val formatName = ("com.highperformancespark.examples" +
      "structuredstreaming.CustomSinkCollectorProvider")
    val query = doubled.writeStream
      .queryName("testCustomSinkBasic")
      .format(formatName)
      .start()
    val inputData = List("hi", "holden", "bye", "pandas")
    input.addData(inputData)
    assert(query.isActive === true)
    query.processAllAvailable()
    assert(query.exception === None)
    assert(Pandas.results(0) === inputData.map(x => x + " " + x))
  }
}

object Pandas{
  val results = new ListBuffer[Seq[String]]()
}

class CustomSinkCollectorProvider extends ForeachDatasetSinkProvider {
  override def func(df: DataFrame) {
    val spark = df.sparkSession
    import spark.implicits._
    Pandas.results += df.as[String].rdd.collect()
  }
}
