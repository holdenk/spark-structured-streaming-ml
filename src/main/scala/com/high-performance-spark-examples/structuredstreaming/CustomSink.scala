/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import org.apache.spark._
import org.apache.spark.sql.{Dataset, DataFrame, Encoder, SQLContext}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.execution.streaming.Sink

/**
 * Creates a custom sink similar to the old foreachRDD. Provided function is called for each
 * time slice with the dataset representing the time slice.
 * Provided func must consume the dataset (e.g. call `foreach` or `collect`).
 * As per SPARK-16020 arbitrary transformations are not supported, but converting to an RDD
 * will allow for more transformations beyond `foreach` and `collect`.
 */
case class ForeachDatasetSinkProvider[T: Encoder](func: Dataset[T] => Unit)
    extends StreamSinkProvider {
  def createSink(sqlContext: SQLContext, params: Map[String, String], cols: Seq[String]) = {
    new ForeachDatasetSink(func)
  }
}

/**
 * Custom sink similar to the old foreachRDD. Do not construct directly, instead provide
 * [[ForeachDatasetSinkProvider]] to Spark's DataStreamWriter format.
 */
case class ForeachDatasetSink[T: Encoder](func: Dataset[T] => Unit)
    extends Sink {
  def addBatch(batchId: Long, data: DataFrame): Unit = {
    func(data.as[T])
  }
}
