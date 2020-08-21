package Dec

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature._
import org.apache.spark.sql.Dataset

object TestML {
  def main(args: Array[String]): Unit = {
  }
  // pipeline for train
  def createPipiline(dataset: Dataset[_]): Pipeline = {
    // step 1 sentence 拆成 words
    val tokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern(",")
    // step 2 label 转化为以0开始的labelIndex 为了适应spark.ml
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndex").fit(dataset)
    // step3 统计tf词频
    val countModel = new CountVectorizer().setInputCol("words").setOutputCol("rawFeatures")
    // step4 tf-idf
    val idfModel = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // step5 normalize tf-idf vector
    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normalizedFeatures")
    // step6 naive bayes model
    val naiveBayes = new NaiveBayes().setFeaturesCol("normalizedFeatures").setLabelCol("labelIndex").setWeightCol("obsWeights").setPredictionCol("prediction").setModelType("multinomial").setSmoothing(1.0)
    // step7 predict label to real label
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(indexer.labels)
    new Pipeline().setStages(Array(tokenizer, indexer, countModel, idfModel, normalizer, naiveBayes, labelConverter))
  }
}
