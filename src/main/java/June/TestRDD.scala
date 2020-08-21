package June

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object TestRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getCanonicalName)


    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("G:\\test\\111.txt", 10)
//    rdd.count()

    val func = (itr: Iterator[String]) => {
      var count = 0
      itr.foreach(each => {
        count += 1
      })
      (TaskContext.getPartitionId(), count + "")
    }


//    sc.runJob(rdd, func).foreach(println)

//     sc.parallelize(List((1,1,1),(1,1,3),(1,1,2))).sortBy(_._3).foreach(println(_))

    sc.stop()

  }
}
