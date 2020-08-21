package Dec.ML

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

object DecisionTreeClassificationExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DecisionTreeClassificationExample").getOrCreate()

    //读取数据集
    //读取LIBSVM格式的文本文件并保存为DataFrame
    val data = spark.read.format("libsvm").load("E:\\software\\spark-2.1.1-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
    //用StringIndexer转换标签列
    //    data.show()
    val labelIndexer = new StringIndexer()
      .setInputCol("label").setOutputCol("indexedLabel").fit(data)
    //用VectorIndexer转换特征列
    //设置最大分类特征数为4
    val featureIndexer = new VectorIndexer()
      .setInputCol("features").setOutputCol("indexFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    //指定执行测量树分类算法的转化器（使用默认参数）
    val dt =new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexFeatures")

    // 用IndexToString把预测的索引列转化成原始标签列
     val labelConverter = new  IndexToString()
       .setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
      //组装Pipeline
    val pipeline =new  Pipeline().setStages(Array(labelIndexer,featureIndexer,dt,labelConverter))
    val model =pipeline.fit(trainingData)
    val predictions = model.transform(testData)
     predictions.select("predictedLabel","label","features").show()

    val evaluator=new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
     val accuracy=evaluator.evaluate(predictions)
     // 输出误差
     println("Test Error = " + (1.0 - accuracy))
    // 从PipelineModel中取出决策树模型treeModel
    //val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    // 输出treeModel的决策过程
   // println("Learned classification tree model:\n" + treeModel.toDebugString)


  }
}
