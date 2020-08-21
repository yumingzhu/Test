import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Demotest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
//    sc.parallelize(List(("张三", 1), ("李四", 2))).toDF("name", "number").createTempView("table")
    spark.createDataFrame(List(("table1",1,"date"),("table2",2,"date"),("table3",3,"date")))
      .toDF("name", "number","time")
     .createTempView("table")
    spark.sql("select  * from table ").show()


    //    val frame = spark.read.format("csv").option("header","true").load("C:\\Users\\Lenovo\\Documents\\WeChat Files\\wxid_89c71j2ma3g922\\FileStorage\\File\\2019-05\\j.csv")
    //    import spark.implicits._
    //    frame.toDF("id","type","num").map(row=>{
    //      val id: String = row.getString(1);
    //      val types: String = row.getString(2).replaceAll(" ","").replaceAll("\t","").replaceAll("-","").toUpperCase()
    //      (id,types)
    //    }).toDF().show()

    //    val frame: DataFrame = spark.read.format("csv").option("header","true").load("C:\\Users\\Lenovo\\Documents\\WeChat Files\\wxid_89c71j2ma3g922\\FileStorage\\File\\2019-05\\爱奇艺_245w.csv")
    //   import spark.implicits._
    //    sc.textFile("C:\\Users\\Lenovo\\Documents\\WeChat Files\\wxid_89c71j2ma3g922\\FileStorage\\File\\2019-05\\j.txt").map(line=>{
    //      val strings: Array[String] = line.split(",")
    //      val id: String = strings(0);
    //      val types: String = strings(1).replaceAll(" ","").replaceAll("\t","").replaceAll("-","").toUpperCase()
    //      val count=strings(2)
    //      (id,types,count)
    //    }).toDF().show(10000)
    //      // .toDF("id","brand","num").write.csv("G://test//今日头条")


  }
  case  class  h(tableName:String,number:Int,date: Date)

}
