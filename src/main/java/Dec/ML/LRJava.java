package Dec.ML;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Array;

public class LRJava {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("LogisticRegressionWithSGDDemo").master("local").getOrCreate();
        JavaSparkContext jc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        sparkSession.read().textFile("G:\\test\\spam.txt").toDF("message").createTempView("span");
        Dataset<Row> span = sparkSession.sql("select  message ,case when 1==1 then 1 else 0  end  as label    from  span ");
        sparkSession.read().textFile("G:\\test\\ham.txt").toDF("message").createTempView("ham");
        Dataset<Row> ham = sparkSession.sql("select  message ,case when 1==1 then 0 else 1  end  as label   from  ham ");
        Dataset<Row> training = span.union(ham);
        Tokenizer tokenizer = new Tokenizer().setInputCol("message").setOutputCol("works");
        HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("works").setOutputCol("features");
        //创建管线  训练数据
        LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01);
        PipelineStage[] pipelineStage = {tokenizer,hashingTF,lr};
        Pipeline pipeline = new Pipeline().setStages(pipelineStage);
        PipelineModel model = pipeline.fit(training);
        Dataset<Row>  test =sparkSession.read().textFile("G:\\test\\eamilTest.txt").toDF("message");
        model.transform(test).show();




    }
}
