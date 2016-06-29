/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import scala.collection.mutable.ListBuffer

import org.apache.spark._
import org.apache.spark.sql.{Dataset, DataFrame, Encoder, SQLContext}

class CustomSinkSuite {
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
