package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{ParamValidators, IntParam, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.StreamingMLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.BLAS
import org.apache.spark.ml.param._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{OutputMode, EvilStreamingQueryManager, StreamingQuery}


trait StreamingKMeansParams extends Params {
  /**
   * The smoothing parameter.
   * (default = 1.0).
   *
   * @group param
   */
  final val k: IntParam = new IntParam(this, "smoothing", "The smoothing parameter.",
    ParamValidators.gtEq(0))

  /** @group getParam */
  final def getK: Int = getOrDefault(k)
}

class StreamingKMeansModel(val centers: Array[Vector], val weights: Array[Double])
  extends Serializable {

  private def clusterCentersWithNorm: Iterable[VectorWithNorm] =
    centers.map(new VectorWithNorm(_))

  def predict(point: Vector): Int = {
    StreamingKMeans.findClosest(clusterCentersWithNorm, new VectorWithNorm(point))._1
  }
}

class StreamingKMeans(val uid: String) extends Serializable  with StreamingKMeansParams {

  def this() = this(Identifiable.randomUID("snb"))
  protected var model: StreamingKMeansModel = _

  /**
   * Set the number of cluster centers.
   * Default is 1.
   *
   * @group setParam
   */
  def setK(value: Int): this.type = set(k, value)
  setDefault(k -> 1)

  protected var clusterCenters: Array[Vector] = _
  protected var clusterWeights: Array[Double] = _
  var isModelUpdated = true

  private[this] def assertInitialized(): Unit = {
    if (clusterCenters == null) {
      throw new IllegalStateException()
    }
  }

  /**
   * Train the model on a streaming DF using evil tricks
   */
  def evilTrain(df: DataFrame): StreamingQuery = {
    assertInitialized()
    val sink = new ForeachDatasetSink({df: DataFrame => update(df)})
    val sparkSession = df.sparkSession
    val evilStreamingQueryManager = EvilStreamingQueryManager(sparkSession.streams)
    evilStreamingQueryManager.startQuery(
      Some(s"skmeans-train-$uid"),
      None,
      df,
      sink,
      OutputMode.Append())
  }

  def update(df: DataFrame): Unit = {
    isModelUpdated = false
    import df.sparkSession.implicits._
    val rdd = df.rdd.map { case Row(point: Vector) => point }
    add(rdd)
  }

  /**
   * Specify initial centers directly.
   */
  def setInitialCenters(centers: Array[Vector], weights: Array[Double]): this.type = {
    require(centers.size == weights.size,
      "Number of initial centers must be equal to number of weights")
    require(centers.size == getK,
      s"Number of initial centers must be ${getK} but got ${centers.size}")
    require(weights.forall(_ >= 0),
      s"Weight for each inital center must be nonnegative but got [${weights.mkString(" ")}]")
    clusterCenters = centers
    clusterWeights = weights
    model = new StreamingKMeansModel(centers, weights)
    this
  }

  /**
   * Initialize random centers, requiring only the number of dimensions.
   *
   * @param dim Number of dimensions
   * @param weight Weight for each center
   * @param seed Random seed
   */
  def setRandomCenters(dim: Int, weight: Double, seed: Long = scala.util.Random.nextLong): this.type = {
    require(dim > 0,
      s"Number of dimensions must be positive but got ${dim}")
    require(weight >= 0,
      s"Weight for each center must be nonnegative but got ${weight}")
    clusterCenters =
      Array.fill(getK)(Vectors.dense(Array.fill(dim)(scala.util.Random.nextGaussian())))
    clusterWeights = Array.fill(getK)(weight)
    model = new StreamingKMeansModel(clusterCenters, clusterWeights)
    this
  }

  def getModel: StreamingKMeansModel = {
    val centers = Array.tabulate(clusterCenters.length) { i =>
      clusterCenters(i).copy
    }
    val weights = clusterWeights.clone()
    new StreamingKMeansModel(centers, weights)
  }


  def add(data: RDD[Vector]): Unit = {
    val closest = data.map(point => (model.predict(point), (point, 1L)))

    // TODO: don't compute this always
    val dim = closest.first()._2._1.size

    val pointStats = closest
      .aggregateByKey((Vectors.zeros(dim), 0L))(mergeContribs, mergeContribs)
      .collect()

    pointStats.foreach { case (label, (sum, count)) =>
      val centroid = clusterCenters(label)

      val updatedWeight = model.weights(label) + count
      val lambda = count / math.max(updatedWeight, 1e-16)

      clusterWeights(label) = updatedWeight
      BLAS.scal(1.0 - lambda, centroid)
      BLAS.axpy(lambda / count, sum, centroid)
    }
  }

  private val mergeContribs: ((Vector, Long), (Vector, Long)) => (Vector, Long) = (p1, p2) => {
    BLAS.axpy(1.0, p2._1, p1._1)
    (p1._1, p1._2 + p2._2)
  }

  override def copy(extra: ParamMap): StreamingKMeans = defaultCopy(extra)

}

object StreamingKMeans {
  def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double =
          StreamingMLUtils.fastSquaredDistance(center.vector, center.norm, point.vector, point.norm)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }
}

class VectorWithNorm(val vector: Vector, val norm: Double) extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** Converts the vector to a dense vector. */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}
