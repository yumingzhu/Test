package Dec.ML

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Dataset, SparkSession}

object LogisticRegressionWithSGDDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("LogisticRegressionWithSGDDemo").master("local").getOrCreate()
//    val sc = sparkSession.sparkContext;
    sparkSession.read.textFile("G:\\test\\spam.txt").toDF("message").createTempView("span")

    val span = sparkSession.sql("select  message ,case when 1==1 then 1 else 0  end  as label    from  span ")

    sparkSession.read.textFile("G:\\test\\ham.txt").toDF("message").createTempView("ham")
    val ham = sparkSession.sql("select  message ,case when 1==1 then 0 else 1  end  as label   from  ham ")
   val training = span.union(ham)
//    span.show()
//    ham.show()
     //使用分词器
    val tokenizer = new  Tokenizer().setInputCol("message").setOutputCol("works")
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("works").setOutputCol("features")
    //创建管线  训练数据
     val lr = new  LogisticRegression().setMaxIter(10).setRegParam(0.01).setThreshold(0.5)
    val pipeline = new  Pipeline().setStages(Array(tokenizer,hashingTF,lr))
    val  model =pipeline.fit(training)
    val  test =sparkSession.read.textFile("file:///G:/test/eamilTest.txt").toDF("message")
     model.transform(test).show()

    //   ("G:\\test\\span.txt")


  }

}
