package Dec.DecTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1, 1), (2, 2), (3, 3), (4, 4)))
    val rdd2 = sc.parallelize(Array((1, "one"), (2, "two"), (3, "three")))
    val value = rdd1.leftOuterJoin(rdd2)

    val data=List((1,3),(1,2),(1,4),(2,3))
    val rdd=sc.parallelize(data )
    //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
    def combOp(a:List[Int],b:List[Int]):List[Int] ={
      a ++ b
    }
    def seqOp(a:List[Int],b:Int):List[Int]={
       List(a(0)+b)
    }
  //  rdd.foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    //rdd.aggre
    val aggregateByKeyRDD=rdd.aggregateByKey(List(0))(seqOp, combOp)
      aggregateByKeyRDD.foreach(println)
    val groupbykeyRDD=rdd.groupByKey()
     groupbykeyRDD.foreach(println)

 //   val combineByKeyRDD = rdd.combineByKey(createCombiner,mergeValue,mergeCombiners)
//    combineByKeyRDD.foreach(println)
    sc.stop()
  }
//    //为所有的元素进行一个map 的操作
//   def  createCombiner = (v:Int) =>  (v:Int, 1)
//  // 聚合当前分区的的数据
//  def   mergeValue = (c:(Int, Int), v:Int) => (c._1 + v, c._2 + 1)
//   // 根据key 对数据进行一整体的聚合操作
//  def   mergeCombiners = (c1:(Int, Int), c2:(Int, Int)) => (c1._1 + c2._1, c1._2 + c2._2)


}
