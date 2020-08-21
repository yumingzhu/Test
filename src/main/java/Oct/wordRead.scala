package Oct

import scala.util.parsing.json.JSON

object wordRead {
  def main(args: Array[String]): Unit = {

    val jsTest = "{\"a\":1, \"b\":\"2\"}"
    println(JSON.parseFull(jsTest).get)
  }

  def regJson(json: Option[Any]): Map[String, Any] = json match {
    case Some(map: Map[String, Any]) => map
  }

}
