package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vector => SparkVector}

case class LabelCount(label: Double, count: Long)
case class LabeledToken(label: Double, value: (Double, Int))
case class LabeledTokenCounts(label: Double, value: (Double, Int), count: Long)

case class QueryBasedStreamingNaiveBayesModel(table: Dataset[LabeledTokenCounts]) {
  import table.sparkSession.implicits._

  def counts(vec: SparkVector) = {
    val tokens = vec.toArray.zip(Stream from 1)
    val totals = table.groupBy($"label").agg(sum($"count").alias("count")).as[LabelCount].collect()
    val relevant = table.filter(r => tokens.contains(r.value)).collect()
    val tokenCounts = relevant.map(r => ((r.value._1, r.value._2, r.label), r.count)).toMap
    val labels = totals.map(_.label)
    val counts = labels.map(label =>
      tokens.map(token =>
        tokenCounts.getOrElse((token._1, token._2, label), 0L)).toList
    )
    (counts.toList, totals.toList)
  }
}


class QueryBasedStreamingNaiveBayes {
  def train(ds: Dataset[LabeledPoint]) = {
    import ds.sparkSession.implicits._
    //tag::simpleTrain[]
    // Compute the counts using a Dataset transformation
    val counts = ds.flatMap{
      case LabeledPoint(label, vec) =>
        vec.toArray.zip(Stream from 1).map(value => LabeledToken(label, value))
    }.groupBy($"label", $"value").agg(count($"value").alias("count"))
      .as[LabeledTokenCounts]
    // Create a table name to store the output in
    val tblName = "qbsnb" + java.util.UUID.randomUUID.toString.filter(_ != '-').toString
    // Write out the aggregate result in complete form to the in memory table
    val query = counts.writeStream.outputMode(OutputMode.Complete())
      .format("memory").queryName(tblName).start()
    val tbl = ds.sparkSession.table(tblName).as[LabeledTokenCounts]
    val model = new QueryBasedStreamingNaiveBayesModel(tbl)
    //end::simpleTrain[]
    (model, query)
  }
}
