package com.highperformancespark.examples.structuredstreaming

import org.apache.spark.ml.linalg.{SparseVector, DenseVector, Vector}

object MLUtils {
  /**
   * a * x + y
   */
  def axpy(a: Double, x: Vector, y: Vector): Unit = {
    y match {
      case dy: DenseVector =>
        x match {
          case dx: DenseVector =>
            var i = 0
            while (i < x.size) {
              y.toArray(i) += x(i) * a
              i += 1
            }
          case sx: SparseVector =>
            throw new NotImplementedError("SparseVector not yet supported")
        }
      case sy: SparseVector =>
        throw new IllegalArgumentException("SparseVector not supported")
    }
  }
}
