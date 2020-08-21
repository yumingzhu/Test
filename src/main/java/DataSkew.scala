import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object DataSkew {
  def main(args: Array[String]): Unit = {



    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val array=Array((1,(1 to 10000000).toList.map(_.toString.toInt)),
      (2,(1 to 10).toList.map(_.toString.toInt)),
      (3,(1 to 10).toList.map(_.toString.toInt)),
      (4,(1 to 10).toList.map(_.toString.toInt)))

    sc.parallelize(array).flatMap(x=>x._2.map(index=>{(x._1,index)})).groupByKey().map(x=> (x._1, x._2.toList.sum)).collect().foreach(println)
  }
}
