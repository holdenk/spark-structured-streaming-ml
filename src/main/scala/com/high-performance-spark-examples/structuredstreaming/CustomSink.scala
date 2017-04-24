/**
 * A simple custom sink to allow us to train our models on micro-batches of data.
 */
package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.execution.streaming.Sink


//tag::foreachDatasetSink[]
/**
 * Creates a custom sink similar to the old foreachRDD. Provided function is
 * called for each time slice with the dataset representing the time slice.
 *
 * Provided func must consume the dataset (e.g. call `foreach` or `collect`).
 * As per SPARK-16020 arbitrary transformations are not supported, but converting
 * to an RDD will allow for more transformations beyond `foreach` and `collect` while
 * preserving the incremental planning.
 *
 */
abstract class ForeachDatasetSinkProvider extends StreamSinkProvider {
  def func(df: DataFrame): Unit

  def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): ForeachDatasetSink = {
    new ForeachDatasetSink(func)
  }
}

/**
 * Custom sink similar to the old foreachRDD.
 * To use with the stream writer - do not construct directly, instead subclass
 * [[ForeachDatasetSinkProvider]] and provide to Spark's DataStreamWriter format.
 *  This can also be used directly as in StreamingNaiveBayes.scala
 */
case class ForeachDatasetSink(func: DataFrame => Unit)
    extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    func(data)
  }
}
//end::foreachDatasetSink[]
//tag::basicSink[]
/**
 * A basic custom sink to illustrate how the custom sink API is currently
 * intended to be used
 */
class BasicSinkProvider extends StreamSinkProvider {
  // Here we don't do any special work because our sink is so simple - but setup
  // work can go here.
  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): BasicSink = {
    new BasicSink()
  }
}

class BasicSink extends Sink {
  /*
   * As per SPARK-16020 arbitrary transformations are not supported, but
   * converting to an RDD allows us to do magic.
   */
  override def addBatch(batchId: Long, data: DataFrame) = {
    val batchDistinctCount = data.rdd.distinct.count()
    println(s"Batch ${batchId}'s distinct count is ${batchDistinctCount}")
  }
}
//end::basicSink[]
object CustomSinkDemo {
  def write(ds: Dataset[_]) = {
    //tag::customSinkDemo[]
    ds.writeStream.format(
      "com.highperformancespark.examples.structuredstreaming." +
        "BasicSinkProvider")
      .queryName("customSinkDemo")
      .start()
    //end::customSinkDemo[]
  }
}
