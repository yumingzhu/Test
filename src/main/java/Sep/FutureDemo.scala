package Sep


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.util.{Failure, Success}

object FutureDemo {


  //  System.setProperty("scala.concurrent.context.minThreads", "16")
  //  System.setProperty("scala.concurrent.context.maxThreads", "16")

  def main(args: Array[String]): Unit = {
    val fut = Future {
      Thread.sleep(1000)
      1 / 0
      1 + 1
    }
    fut onComplete {
      case Success(r) => println(s"the result is ${r}")
      case Failure(r) => println("failure " + r)
    }
    println(fut.isCompleted)
    println("I am working")
    Thread.sleep(2000)


    //    val result = Future(println("xxx"))

    //    val mFuture = Future {
    //      Thread.sleep(1000)
    //      "result function"
    //    }
    //    println(mFuture)

    //    val result = Await result(mFuture, 3.seconds)
    //    println(result)


  }

  private def printFuture = {
    (1 to 20).foreach(index => Future {
      println(Thread.currentThread().getName + "," + index)
      Thread.sleep(1000)
    })

    println("AvailableProcessors: " + Runtime.getRuntime.availableProcessors()) //打印出可用处理器数目
    Thread.sleep(20000) //为了在程序结束前看到执行完所有任务，所以阻塞主线程
  }
}
