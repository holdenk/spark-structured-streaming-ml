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

  protected implicit def impSqlContext: SQLContext = sqlContext

  test("really simple test of the custom sink") {
    import spark.implicits._
    val input = MemoryStream[String]
    val doubled = input.toDS().map(x => x + " " + x)
    doubled.writeStream
      .queryName("testCustomSinkBasic")
      .format("com.highperformancespark.examples.structuredstreaming")
      .start()
    input.addData(List("hi", "holden", "bye", "pandas"))
  }
}

/**
 * Collect the results after converting to an RDD
 */
class CustomSinkCollector {
  val results = new ListBuffer[Seq[String]]()
  def addFunc(data: Dataset[String]) = {
    results += data.rdd.collect()
  }
}
