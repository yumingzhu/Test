package Dec.ML

import breeze.linalg.{DenseMatrix, DenseVector}

object breezeDemo {
  def main(args: Array[String]): Unit = {
    val m1 = DenseMatrix.zeros[Double](2, 3);
//    println(m1)
    val v1 = DenseVector.zeros[Double](3);
//    println(v1)
    val v2=DenseVector.ones[Double](3)
//    println(v2)
    val v3=DenseVector.fill(3){5.0}
//   println(v3)
   val  v4=DenseVector.range(1,10 ,2)
    println(v4)
   val  m2=DenseMatrix.eye[Double](4)
    println(m2)


  }


}
