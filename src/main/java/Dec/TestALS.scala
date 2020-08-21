package Dec

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object TestALS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("TestALS").getOrCreate()
    val sc = spark.sparkContext
    val rdd = sc.textFile("E:\\software\\spark-2.1.1-bin-hadoop2.7\\data\\mllib\\als\\sample_movielens_ratings.txt")
    import  spark.implicits._
    val ratings = rdd.map(line => {
      val fields = line.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }).toDF()
    ratings.show()
    val Array(training,test) = ratings.randomSplit(Array(0.8,0.2))
   val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    val alsImplicit = new ALS().setMaxIter(5).setRegParam(0.01).setImplicitPrefs(true). setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    val modelExplicit = alsExplicit.fit(training)
    val modelImplicit = alsImplicit.fit(training)
    val predictionsExplicit = modelExplicit.transform(test)
    val predictionsImplicit = modelImplicit.transform(test)
   //模型的评估， 使用均方根误差来对模型进行评估，  均方根误差越小， 模型越准确
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating"). setPredictionCol("prediction")
    val rmseExplicit = evaluator.evaluate(predictionsExplicit)
    val rmseImplicit = evaluator.evaluate(predictionsImplicit)
       println(rmseExplicit)
       println(rmseImplicit)
  }

  case class  Rating(userId:Int,movieId:Int,rating:Float,timestamp:Long)

}
