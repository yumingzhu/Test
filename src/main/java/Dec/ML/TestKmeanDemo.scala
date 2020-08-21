package Dec.ML

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.SparkSession

object TestKmeanDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("TestKmeans").getOrCreate()
    val sc = spark.sparkContext
    //装载数据集
    val data = sc.textFile("G:\\test\\ml\\iris.txt")
    import spark.implicits._
    val df = data.map(line => {
      model_instance(Vectors.dense(line.split(",").filter(p => p.matches("\\d*(\\.?)\\d*")).map(_.toDouble)))
    }).toDF()
    df.show()
    val kmeansmodel = new KMeans().
      setK(3).setFeaturesCol("features").
      setPredictionCol("prediction").
      fit(df)
    val results = kmeansmodel.transform(df)
    results.collect().foreach(row => {
      println(row(0) + " is predicted as cluster " + row(1))
    })
    kmeansmodel.clusterCenters.foreach(center => {
      println("Clustering Center:" + center)
    })
  }

}

case class model_instance(features: Vector)

