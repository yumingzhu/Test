package Dec.ML

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeanDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //装载数据集
    val data = sc.textFile("G:\\test\\kmeans_data.txt")
    val parsedData = data.map(s => {
      Vectors.dense(s.split(" ").map(_.toDouble))
    })
    //将数据集聚类， 两个类， 20次迭代， 进行模型训练形成数据模型
    val numCluster = 2;
    val numlterations = 20
    val model = KMeans.train(parsedData, numCluster, numlterations)
    println("----------")
    // 打印数据模型的中心点

    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }
    // 使用误差平方之和来评估数据模型
    val cost = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + cost)
    // 使用模型测试单点数据
    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
    println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))


  }


}
