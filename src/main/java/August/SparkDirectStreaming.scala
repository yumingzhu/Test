package August

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler}
import org.spark_project.jetty.server.{Request, Server}

/**
  * Created by QinDongLiang on 2017/11/28.
  */
object SparkDirectStreaming {
  System.setProperty("HADOOP_USER_NAME", "root")

  System.setProperty("user.name", "root")

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  /** *
    * 创建StreamingContext
    *
    * @return
    */
  def createStreamingContext(): StreamingContext = {

    val isLocal = true
    //是否使用local模式
    val firstReadLastest = true //第一次启动是否从最新的开始消费

    val sparkConf = new SparkConf().setAppName("Direct Kafka stop")
    if (isLocal) sparkConf.setMaster("local[2]") //local模式
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") //优雅的关闭


    val ssc = new StreamingContext(sparkConf, Seconds(10)) //创建StreamingContext,每隔多少秒一个批次

    //ReceiverInputDStream就是个DStream，继承InputDStream继承DStream(就是一个抽象类,其实就是个HashMap(Time，RDD[T])一个时间点对应一个RDD )
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.52", 9999)
    //开始处理数据
    dstream.foreachRDD(rdd => {
      rdd.flatMap(_.split(",").map((_, 1))).reduceByKey(_ + _).foreach(println(_))
    })


    ssc //返回StreamContext


  }


  /** **
    * 负责启动守护的jetty服务
    *
    * @param port 对外暴露的端口号
    * @param ssc  Stream上下文
    */
  def daemonHttpServer(port: Int, ssc: StreamingContext) = {
    val server = new Server(port)
    val context = new ContextHandler();
    context.setContextPath("/close");
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  /** * 负责接受http请求来优雅的关闭流
    *
    * @param ssc Stream上下文
    */
  class CloseStreamHandler(ssc: StreamingContext) extends AbstractHandler {
    override def handle(s: String, baseRequest: Request, req: HttpServletRequest, response: HttpServletResponse): Unit = {
      log.warn("开始关闭......")
      ssc.stop(true, true) //优雅的关闭
      response.setContentType("text/html; charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      val out = response.getWriter();
      out.println("close success");
      baseRequest.setHandled(true);
      log.warn("关闭成功.....")
    }
  }


  /** *
    * 通过一个消息文件来定时触发是否需要关闭流程序
    *
    * @param ssc StreamingContext
    */
  def stopByMarkFile(ssc: StreamingContext): Unit = {
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfs_file_path = "/user/root/xxx" //判断消息文件是否存在，如果存在就
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (!isStop && isExistsMarkFile(hdfs_file_path)) {
        log.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }

    }
  }

  /** *
    * 判断是否存在mark file
    *
    * @param hdfs_file_path mark文件的路径
    * @return
    */
  def isExistsMarkFile(hdfs_file_path: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfs_file_path)
    val fs = path.getFileSystem(conf);
    println("path")
    fs.exists(path)
  }


  def main(args: Array[String]): Unit = {

    //创建StreamingContext
    val ssc = createStreamingContext()
    //开始执行
    ssc.start()

    //启动接受停止请求的守护进程
    //    daemonHttpServer(5555, ssc) //方式一通过Http方式优雅的关闭策略


    stopByMarkFile(ssc) //方式二通过扫描HDFS文件来优雅的关闭


    //等待任务终止
    ssc.awaitTermination()


  }


}