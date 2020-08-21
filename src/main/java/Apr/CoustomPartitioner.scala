package Apr

import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}

/** *
  *
  */
object CoustomPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streaming Jason").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 10,5)
    rdd.map((_,1)).partitionBy(new MyPartitioner(2)).foreachPartition(fp=>{
      println("分区ID:" + TaskContext.get.partitionId)
      fp.foreach(f=>{
        println(f)
      })
    })


  }

  class MyPartitioner(num: Int) extends Partitioner {
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
       return  key.toString().toInt % num
    }
  }


}
