package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.sql.streaming._
import org.apache.spark.sql._

import org.apache.spark.ml.feature._

case class LabeledToken(label: Double, value: (Double, Int))
case class LabeledTokenCounts(label: Double, value: (Double, Int), count: Long)

case class QueryBasedStreamingNaiveBayesModel() {
  val scores = new scala.collection.mutable.HashMap[LabeledToken, Long]()
  def update(element: LabeledTokenCounts) = {
    val lt = LabeledToken(element.label, element.value)
    val count = scores.getOrElse(lt, 0L)
    scores.update(lt, count + element.count)
  }
}


class QueryBasedStreamingNaiveBayes {
  def train(ds: Dataset[LabeledPoint]): QueryBasedStreamingNaiveBayesModel = {
    import ds.sparkSession.implicits._
    val counts = ds.flatMap{
      case LabeledPoint(label, vec) =>
        vec.toArray.zip(Stream from 1).map(value => LabeledToken(label, value))
    }.groupBy($"value", $"value").agg($"value").as[LabeledTokenCounts]
    val model = new QueryBasedStreamingNaiveBayesModel()
    val foreachWriter: ForeachWriter[LabeledTokenCounts] =
      new ForeachWriter[LabeledTokenCounts] {
        def open(partitionId: Long, version: Long): Boolean = {
          // always open
          true
        }
        def close(errorOrNull: Throwable): Unit = {
          // No close logic - if we wanted to copy updates per-batch
          // we could do that here
        }
        def process(record: LabeledTokenCounts): Unit = {
          model.update(record)
        }
      }
    val query = counts.writeStream.foreach(foreachWriter)
    query.start()
    model
  }
}
