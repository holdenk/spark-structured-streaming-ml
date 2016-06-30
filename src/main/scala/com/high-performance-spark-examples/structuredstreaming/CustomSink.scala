/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import com.high.performance.spark.examples.structuredstreaming.StreamingNaiveBayes
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.execution.streaming.Sink


/**
 * Creates a custom sink similar to the old foreachRDD. Provided function is called for each
 * time slice with the dataset representing the time slice.
 * Provided func must consume the dataset (e.g. call `foreach` or `collect`).
 * As per SPARK-16020 arbitrary transformations are not supported, but converting to an RDD
 * will allow for more transformations beyond `foreach` and `collect` while preserving the
 * incremental planning.
 */
class ForeachDatasetSinkProvider extends StreamSinkProvider {
  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): ForeachDatasetSink = {
    new ForeachDatasetSink(Map.empty[String, String])
  }
}

/**
 * Custom sink similar to the old foreachRDD. Do not construct directly, instead provide
 * [[ForeachDatasetSinkProvider]] to Spark's DataStreamWriter format.
 */
case class ForeachDatasetSink(parameters: Map[String, String]) extends Sink {
  val estimator = new StreamingNaiveBayes

  def addBatch(batchId: Long, data: DataFrame): Unit = {
    estimator.update(data)
    if (estimator.hasModel) {
      println(estimator.getModel.pi)
      println(estimator.getModel.theta)
    }
  }
}

class DefaultSource extends ForeachDatasetSinkProvider
