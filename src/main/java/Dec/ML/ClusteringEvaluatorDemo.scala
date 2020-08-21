package Dec.ML

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession


object ClusteringEvaluatorDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("ClusteringEvaluatorDemo").getOrCreate()
    //读取数据集
    //读取LIBSVM格式的文本文件并保存为DataFrame
    val dataset = spark.read.format("libsvm").load("E:\\software\\spark-2.1.1-bin-hadoop2.7\\data\\mllib\\sample_kmeans_data.txt")

    val kmeans = new KMeans().setK(2).setSeed(1L);
    val mode = kmeans.fit(dataset)
    val predictions = mode.transform(dataset)
    predictions.show()

  }
}
