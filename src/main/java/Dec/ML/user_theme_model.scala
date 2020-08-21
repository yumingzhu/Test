package Dec.ML

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object user_theme_model {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("\001")
    assert(fields.size == 6)
    Rating(fields(1).toInt, fields(3).toInt, fields(4).toFloat, fields(5).toLong)
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("user_theme_model")
      //.master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val inputdata = args(0).toString
    val outputdata = args(1).toString
    val MaxIter = args(2).toInt
    val RegParam = args(3).toFloat

    // $example on$
    val ratings = spark.read.textFile(inputdata).map(parseRating).toDF()
    // 筛选出安装主题数大于N的用户，有大部分用户只安装了一个主题，特征太少，干掉了。
    ratings.createOrReplaceTempView("input_data")
    val result_valuse = spark.sql("SELECT uts.userId,uts.movieId,uts.rating,uts.timestamp from(SELECT userId from input_data GROUP BY userId HAVING count(userId)>4) TABLE1 join input_data uts on (uts.userId = TABLE1.userId)")
    val ratings2 = result_valuse.toDF()
    ratings2.show()

    //将数据集切分为训练集和测试集
    val Array(training, test) = ratings2.randomSplit(Array(0.9, 0.1))

    //使用ALS在训练集数据上构建推荐模型
    val als = new ALS()
      .setMaxIter(MaxIter)
      .setRegParam(RegParam)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
    //为确保不获取到NaN评估参数，我们将冷启动策略设置为drop。
    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)

    // 通过计算rmse(均方根误差)来评估模型
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    //推荐前100个主题
    val userRecs = model.recommendForAllUsers(100)
    val dataout = userRecs.rdd.map(x => x.get(0) + "\001" + x.get(1))
    dataout.saveAsTextFile(outputdata)
    //userRecs.show()
    spark.stop()
  }
}