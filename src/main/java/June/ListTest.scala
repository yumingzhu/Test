package June

import scala.util.control.Breaks._

object ListTest {
  def main(args: Array[String]): Unit = {





  }



  private def test = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    println(list)
    val length = 7;
    println(list.slice(0, if (length < list.size) length else list.size))
    breakable(list.foreach(x => {
      if (x == 5) {
        break()
      }
      println(x)
    })
    )
    //    val a: (Int => (Int, Double)) = (a) => (a, a.toDouble)

    //    list.map(a).foreach(println(_))
    val f1 = nulBy(1)
    val f2 = nulBy(2);
    val value1: Double = f1(1)
    val value2: Double = f2(1)
    println(value1 + "\t" + value2)
  }

  def nulBy(factor: Double) = {
    x: Double => x * factor
  }

}

