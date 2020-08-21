package Dec.ML

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("user_theme_model")
      .master("local[4]")
      .getOrCreate()
    import spark.implicits._
    val df = spark.createDataFrame(Seq(
      (0, 0, 1.0),
      (1, 0, 1.0),
      (2, 0, 1.0),
      (3, 0, 1.0),
      (0, 1, 2.0),
      (1, 1, 2.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (0, 2, 3.0),
      (1, 2, 3.0),
      (2, 2, 3.0),
      (0, 3, 1.0),
      (1, 3, 1.0),
      (3, 3, 4.0)
    ))
    val matrix = new CoordinateMatrix(df.map(row => MatrixEntry(row.getAs[Integer](0).toLong, row.getAs[Integer](1).toLong, row.getAs[Double](2))).rdd)
    // 调用sim方法
    val x = matrix.toRowMatrix().columnSimilarities()
    // 得到相似度结果
    x.entries.collect().foreach(println)
  }
}
