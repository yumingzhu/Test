package June

import org.apache.spark.util.AccumulatorV2

class LogAccumulator extends AccumulatorV2[String, java.util.Map[String,Integer]] {
  private val _mapTag: java.util.Map[String, Integer] = new java.util.HashMap[String, Integer]()

  override def isZero: Boolean = {
    _mapTag.isEmpty
  }

  override def reset(): Unit = {
    _mapTag.clear()
  }

  override def add(v: String): Unit = {
    if (_mapTag.get(v) != null) {
      _mapTag.put(v, _mapTag.get(v) + 1)
    } else {
      _mapTag.put(v, 1)
    }

  }

  override def merge(other: AccumulatorV2[String, java.util.Map[String, Integer]]): Unit = {
    other match {
      case o: LogAccumulator => _mapTag.putAll(o.value)
    }
  }

  override def value: java.util.Map[String, Integer] = {
    java.util.Collections.unmodifiableMap(_mapTag)
  }

  override def copy(): AccumulatorV2[String, java.util.Map[String, Integer]] = {
    val newAcc = new LogAccumulator()
    _mapTag.synchronized {
      newAcc._mapTag.putAll(_mapTag)
    }
    newAcc
  }
}



import scala.collection.JavaConversions._

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "7cd", "9"), 2).foreach(line => {
        accum.add(line)
    })


    for (v <- accum.value) print(v + " ")
    println()
    sc.stop()
  }
}

