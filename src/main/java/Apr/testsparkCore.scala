package Apr

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.GenSeq

object testsparkCore {
  val  x:Int=10
  def  setpartition(path:String,minPartitions: Int=x): Unit ={
      println(path+"\t"+minPartitions)

  }
  //偏函数， 可以使用case 充当过滤条件  传入参数， 返回结果类型
  def  inc:PartialFunction[Any,Int]={
    case  i:Int =>i+1;
  }

  def main(args: Array[String]): Unit = {
    val a = Array("Hello", "World")
    val b = Array("hello", "world")
    println(a.corresponds(b)(_.equalsIgnoreCase(_)))


    val sparkConf = new SparkConf().setMaster("local").setAppName("SparkEsToHdfs").set("spark.shuffle.manager", "sort")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate

    val sc=spark.sparkContext
//    sc.setLogLevel("INFO")
    //    sc.textFile("G:\\test\\name.txt").map(a=>a).collect()
    val unit = sc.parallelize(List[(Int,Int)]((1,5),(1,3),(1,4),(2,2),(2,5),(2,1)))
    unit.groupByKey().foreach(x=>println(x))
  }





}
