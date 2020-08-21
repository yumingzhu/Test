package Oct

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/** *
  * 处理日志 公共的 工具类
  */
object LogUtils {
  /**
    * 得到有用的数据
    *
    * @param message
    * @return
    */
  def getUseableMessage(message: String): (String, String) = {
    val begin: Int = message.indexOf("{")
    val end: Int = message.lastIndexOf("}")
    if (begin == -1 || end == -1) {
      return null
    }
    val value: Int = message.indexOf("]")
    val time = message.substring(1, value);
    val json: String = message.substring(begin, end + 1)
    (time, json)
  }





}
