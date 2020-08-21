package August

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化程序入口
      */
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("NetWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))


    //ReceiverInputDStream就是个DStream，继承InputDStream继承DStream(就是一个抽象类,其实就是个HashMap(Time，RDD[T])一个时间点对应一个RDD )
    val dstream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.52", 9999)

    /**
      * 对DStream流进行操作
      */
    //下面都是Transformation操作
    dstream.map(str => {
      val jsonObject: JSONObject = JSON.parseObject(getUseableMessage(str))
      val did: String = jsonObject.getString("did")



    })

    //output的操作类似于RDD的Action
    //    wordCountDStream.print()  //把数据打印出来
    /**
      * 启动应用程序（固定操作）
      */
    //启动我们的程序
    ssc.start();
    //等待结束
    ssc.awaitTermination();
    //如果结束就释放资源
    ssc.stop();


  }
  def getUseableMessage(message: String): String = {
    val begin: Int = message.indexOf("{")
    val end: Int = message.lastIndexOf("}")
    if (begin == -1 || end == -1) {
      return null
    }
    message.substring(begin, end + 1)
  }

}
