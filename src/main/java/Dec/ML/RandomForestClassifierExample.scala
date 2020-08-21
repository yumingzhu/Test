package Dec.ML

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

object RandomForestClassifierExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DecisionTreeClassificationExample").getOrCreate()

    //读取数据集
    //读取LIBSVM格式的文本文件并保存为DataFrame
    val data = spark.read.format("libsvm").load("E:\\software\\spark-2.1.1-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
    //将数据由String 转化为 标签类型
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    // 将features 转化为 Vector
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures")
      .setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)

    val laberConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels);
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, laberConverter));
    val mode1 = pipeline.fit(trainingData)
    val predictions = mode1.transform(testData)
    predictions.select("predictedLabel", "label", "features").show(5);
    val evalutor = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel")
      .setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evalutor.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")
  }
}
