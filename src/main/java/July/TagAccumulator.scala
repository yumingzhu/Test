package July

import org.apache.spark.util.AccumulatorV2

/**
  * Description   自定义累加器
  * Author  余明珠
  * Date 2019/6/27
  */
class TagAccumulator extends AccumulatorV2[String, collection.mutable.Map[String, Long]] {
  private val _mapTag = new collection.mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    _mapTag.isEmpty
  }

  override def reset(): Unit = {
    _mapTag.clear()
  }

  override def add(v: String): Unit = {
    _mapTag.put(v, _mapTag.getOrElse(v, 0L) + 1L)

  }

  override def merge(other: AccumulatorV2[String, collection.mutable.Map[String, Long]]): Unit = {
    other match {
      case o: TagAccumulator => o.value.foreach(entry => {
        _mapTag.put(entry._1, _mapTag.getOrElse(entry._1, 0L) + entry._2)
      })
    }
  }

  override def value: collection.mutable.Map[String, Long] = {
    _mapTag
  }

  override def copy(): AccumulatorV2[String, collection.mutable.Map[String, Long]] = {
    val newAcc = new TagAccumulator()
    _mapTag.synchronized {
      _mapTag.foreach(entry => {
        newAcc._mapTag.put(entry._1, newAcc._mapTag.getOrElse(entry._1, 0L) + entry._2)
      })
    }
    newAcc
  }
}

