package Sep

import java.util.{Timer, TimerTask}

import scala.concurrent._

object TimedEvent {

  def main(args: Array[String]): Unit = {
    val timer = new Timer

    def delayedSucess[T](secs: Int, value: T): Future[T] = {
      val resutl = Promise[T]
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          resutl.success(value)
        }
      }, secs * 1000)
      resutl.future
    }
  }

}
