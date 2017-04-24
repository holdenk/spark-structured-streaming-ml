package com.highperformancespark.examples.structuredstreaming



import org.apache.spark.SparkException
import org.apache.spark.ml.classification.ProbabilisticClassificationModel
import org.apache.spark.sql.streaming._
import org.apache.spark.ml.classification.{
  ProbabilisticClassifier, ProbabilisticClassificationModel}
import org.apache.spark.sql.streaming.OutputMode

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.ml.param._
import org.apache.spark.BLAS

trait StreamingNaiveBayesParams extends Params {
  /**
   * The smoothing parameter.
   * (default = 1.0).
   *
   * @group param
   */
  final val smoothing: DoubleParam = new DoubleParam(this,
    "smoothing",
    "The smoothing parameter.",
    ParamValidators.gtEq(0))

  /** @group getParam */
  final def getSmoothing: Double = getOrDefault(smoothing)
}

class StreamingNaiveBayesModel(
  val uid: String,
  val pi: Vector,
  val theta: Matrix) extends
    ProbabilisticClassificationModel[Vector, StreamingNaiveBayesModel]
  with StreamingNaiveBayesParams {
  // TODO: it would be nice if we could inherit from NaiveBayesModel

  override val numFeatures: Int = theta.numCols

  override val numClasses: Int = pi.size

  private def multinomialCalculation(features: Vector) = {
    val prob = theta.multiply(features)
    BLAS.axpy(1.0, pi, prob)
    prob
  }

  override protected def predictRaw(features: Vector): Vector = {
    multinomialCalculation(features)
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        val maxLog = dv.values.max
        while (i < size) {
          dv.values(i) = math.exp(dv.values(i) - maxLog)
          i += 1
        }
        val probSum = dv.values.sum
        i = 0
        while (i < size) {
          dv.values(i) = dv.values(i) / probSum
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def copy(extra: ParamMap): StreamingNaiveBayesModel = {
    copyValues(
      new StreamingNaiveBayesModel(
        uid,
        pi,
        theta).setParent(this.parent),
      extra)
  }

}

class StreamingNaiveBayes (override val uid: String)
    extends ProbabilisticClassifier[Vector,
      StreamingNaiveBayes,
      StreamingNaiveBayesModel]
  with StreamingNaiveBayesParams with Serializable {

  def this() = this(Identifiable.randomUID("snb"))

  /**
   * HashMap with keys being class labels, values are
   * (numInstancesWithClassLabel, cumulativeTermFrequenciesForClass)
   *
   * Note: this is stored as a mutable map so that we don't need to know the
   * number of outcome classes beforehand. Alternately, the class could take
   * `numLabels` as a parameter and store this as an array, which is how it
   * is done in the batch algo.
   */
  protected val countsByClass = new collection.mutable.HashMap[Double,
    (Long, DenseVector)]

  /**
   * Set the smoothing parameter.
   * Default is 1.0.
   *
   * @group setParam
   */
  def setSmoothing(value: Double): this.type = set(smoothing, value)
  setDefault(smoothing -> 1.0)

  def hasModel = countsByClass.nonEmpty

  private var isModelUpdated = true

  private var latestModel: Option[StreamingNaiveBayesModel] = None


  /**
   * Train the model on a streaming DF using evil tricks
   */
  //tag::evilTrain[]
  def evilTrain(df: DataFrame): StreamingQuery = {
    val sink = new ForeachDatasetSink({df: DataFrame => update(df)})
    val sparkSession = df.sparkSession
    val evilStreamingQueryManager = EvilStreamingQueryManager(sparkSession.streams)
    evilStreamingQueryManager.startQuery(
      Some(s"snb-train-$uid"),
      None,
      df,
      sink,
      OutputMode.Append())
  }
  //end::evilTrain[]

  override protected def train(dataset: Dataset[_]): StreamingNaiveBayesModel = {
    // TODO: actually implement this method
    getModel
  }

  /**
   * Update the class counts with a new chunk of labeled point data.
   *
   * @param df Dataframe to add
   */
  def update(df: DataFrame): Unit = {
    isModelUpdated = false
    import df.sparkSession.implicits._
    val data = df.as[LabeledPoint].rdd
    val newCountsByClass = add(data)
    merge(newCountsByClass)
  }


  /**
   * Get the log class probabilities and prior probabilities from the
   * aggregated counts.
   */
  def getModel: StreamingNaiveBayesModel = {
    val lambda = getSmoothing
    val numLabels = countsByClass.size
    var numDocuments = 0L
    countsByClass.foreach { case (_, (n, _)) =>
      numDocuments += n
    }
    val numFeatures = countsByClass.head match { case (_, (_, v)) => v.size }

    val labels = new Array[Double](numLabels)
    val pi = new Array[Double](numLabels)
    val theta = Array.fill(numLabels)(new Array[Double](numFeatures))

    val piLogDenom = math.log(numDocuments + numLabels * lambda)
    var i = 0
    countsByClass.toArray.sortBy(_._1).foreach { case (label, (n, sumTermFreqs)) =>
      labels(i) = label
      pi(i) = math.log(n + lambda) - piLogDenom
      val thetaLogDenom = math.log(sumTermFreqs.values.sum + numFeatures * lambda)
      var j = 0
      while (j < numFeatures) {
        theta(i)(j) = math.log(sumTermFreqs(j) + lambda) - thetaLogDenom
        j += 1
      }
      i += 1
    }
    new StreamingNaiveBayesModel(Identifiable.randomUID("snb"),
      Vectors.dense(pi),
      new DenseMatrix(labels.length, theta(0).length, theta.flatten, true))
  }

  /**
   * Combine the current class counts with aggregated class counts from a new chunk
   * of data.
   */
  def merge(update: Array[(Double, (Long, DenseVector))]): Unit = {
    update.foreach { case (label, (numDocs, termCounts)) =>
      countsByClass.get(label) match {
        case Some((n, c)) =>
          BLAS.axpy(1.0, termCounts, c)
          countsByClass(label) = (n + numDocs, c)
        case None =>
          // new label encountered
          countsByClass += (label -> (numDocs, termCounts))
      }
    }
  }

  /**
   * Get class counts for a new chunk of data.
   */
  def add(data: RDD[LabeledPoint]): Array[(Double, (Long, DenseVector))] = {
    data.map(lp => (lp.label, lp.features)).combineByKey[(Long, DenseVector)](
      createCombiner = (v: Vector) => {
        (1L, v.copy.toDense)
      },
      mergeValue = (c: (Long, DenseVector), v: Vector) => {
        // TODO: deal with sparse
        BLAS.axpy(1.0, v.toDense, c._2)
        (c._1 + 1L, c._2)
      },
      mergeCombiners = (c1: (Long, DenseVector), c2: (Long, DenseVector)) => {
        BLAS.axpy(1.0, c2._2, c1._2)
        (c1._1 + c2._1, c1._2)
      }
    ).collect()
  }

  /**
   * Transform a dataframe using the latest model.
   * @param dataset input dataset
   * @return transformed dataset
   */
  def transform(dataset: DataFrame): DataFrame = {
    latestModel match {
      case Some(model) =>
        if (!isModelUpdated) {
          latestModel = Some(getModel)
        }
      case None =>
        throw new SparkException("transform called before model was trained")
    }
    latestModel.get.transform(dataset)
  }

  override def copy(extra: ParamMap): StreamingNaiveBayes = defaultCopy(extra)
}


// These two objects are broken up for inclusion in the book they can be merged.
object SimpleStreamingNaiveBayesTrain {
  //tag::goodTrain[]
  // Train using the model inside SimpleStreamingNaiveBayes object
  // - if called on multiple streams all streams will update the same model :(
  // or would except if not for the hard coded query name preventing multiple
  // of the same running.
  def train(ds: Dataset[_]) = {
    ds.writeStream.format(
      "com.highperformancespark.examples.structuredstreaming." +
        "StreamingNaiveBayesSinkProvider")
      .queryName("trainingnaiveBayes")
      .start()
  }
  //end::goodTrain[]
}
//tag::sbsp[]
object SimpleStreamingNaiveBayes {
  val model = new StreamingNaiveBayes()
}

class StreamingNaiveBayesSinkprovider extends ForeachDatasetSinkProvider {
  override def func(df: DataFrame) {
    val spark = df.sparkSession
    SimpleStreamingNaiveBayes.model.update(df)
  }
}
//end::sbsp[]
