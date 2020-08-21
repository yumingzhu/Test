package javaTest

import java.util


import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{Row, SparkSession}

object CosineSimilarityDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
    val sc = spark.sparkContext
    val list: List[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = List(("deviceid1", 1, 0, 0, 1, 0, 0, 0, 0, 0.2),
      ("deviceid2", 0, 1, 0, 0, 0, 1, 0, 0, 0.8),
      ("deviceid3", 0.6, 0.4, 0, 1, 0, 0, 0, 0, 0.65),
      ("deviceid4", 1, 0, 0.1, 0.3, 0.35, 0.15, 0.05, 0.05, 10))
    val UserRDD = sc.parallelize(list).map {
      case (id, man, woman, age0, age1, age2, age3, age4, age5, spending) => User(id, man, woman, age0, age1, age2, age3, age4, age5, spending)
    }
    val df = spark.createDataFrame(UserRDD)
     df.registerTempTable("user");
    df.registerTempTable("us");
    spark.sql("select user.*,us.*,sqrt(us.spending) from user  join  us on  user.id <>  us.id").show()
      val StirngIndex = new StringIndexer().setInputCol("id").setOutputCol("idIndex");








  }
  case  class   User(id:String,man:Double,woman:Double,age0:Double,age1:Double,age2:Double,age3:Double,age4:Double,age5:Double,spending:Double)
}
