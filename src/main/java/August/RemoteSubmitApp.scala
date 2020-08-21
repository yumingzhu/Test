package August

import org.apache.spark.{SparkConf, SparkContext}

object RemoteSubmitApp {
  def main(args: Array[String]): Unit = {
    // 设置提交任务的用户
    //    System.setProperty("HADOOP_USER_NAME", "root")
    //    System.setProperty("user.name", "root")

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
      .setMaster("yarn")
      // 设置yarn-client模式提交
      .set("deploy-mode", "client")
      // 设置resourcemanager的ip
      .set("yarn.resourcemanager.hostname", "192.168.1.38")
      // 设置executor的个数
      .set("spark.executor.instance", "2")
      //      // 设置executor的内存大小
      .set("spark.executor.memory", "2G")
      //
      .set("spark.executor.cores", "3")
      //
      .set("spark.driver.memory", "1G")
      .set("spark.yarn.jars", "hdfs://nameservice1/user/root/jars/*") //集群的jars包,是你自己上传上去的

      // 设置提交任务的yarn队列
      //      .set("spark.yarn.queue", "spark")
      // 设置driver的ip地址
      .set("spark.driver.host", "192.168.2.132")
      // 设置jar包的路径,如果有其他的依赖包,可以在这里添加,逗号隔开
      .setJars(List("file:///F:/work/Test/target/Test-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    println(sc.parallelize(1 to 1000).reduce(_ + _))
    //
    sc.stop()


  }
}
