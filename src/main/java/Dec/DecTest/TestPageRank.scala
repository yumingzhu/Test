package Dec.DecTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TestPageRank {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("GraphXTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //图的初始化
    val links = sc.parallelize(
      Array(
        ('A', Array('D')),
        ('B', Array('A')),
        ('C', Array('A', 'B')),
        ('D', Array('A', 'C'))
      )
    )

    //PR值的初始化
    //这里可以用 var ranks  = links.mapValues(_=> 1.0)代替
    var ranks = sc.parallelize(
      Array(
        ('A', 1.0),
        ('B', 1.0),
        ('C', 1.0),
        ('D', 1.0)
      )
    )

    //6 为循环次数,这里可以自己设置
    for (i <- 1 to 6) {
      val joinRdd = links.join(ranks) //连接两个rdd
      //计算来自其他网页的PR 贡献值
      val contribsRdd = joinRdd.flatMap {
        // 注意这里的links为模式匹配得到的值, 类型为Array[Char], 并非前面的ParallelCollectionRDD
        case (srcURL, (links, rank)) => links.map(destURL => (destURL, rank / links.size))
      }
      //ranks进行更新
      ranks = contribsRdd.reduceByKey(_ + _).mapValues(0.15 + _ * 0.85)
      //打印出ranks的值
      ranks.take(4).foreach(println)
      println() //换行，便于观察
    }
  }


}
