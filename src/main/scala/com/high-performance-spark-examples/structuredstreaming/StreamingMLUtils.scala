package org.apache.spark.mllib

import scala.language.implicitConversions

import org.apache.spark.ml.linalg.{SparseVector, DenseVector, Vector}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.util.MLUtils

object StreamingMLUtils {
  implicit def mlToMllibVector(v: Vector): OldVector = v match {
    case dv: DenseVector => OldVectors.dense(dv.toArray)
    case sv: SparseVector => OldVectors.sparse(sv.size, sv.indices, sv.values)
    case _ => throw new IllegalArgumentException
  }

  def fastSquaredDistance(x: Vector, xNorm: Double, y: Vector, yNorm: Double) = {
    MLUtils.fastSquaredDistance(x, xNorm, y, yNorm)
  }
}
