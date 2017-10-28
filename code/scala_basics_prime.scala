package basics

/**
  * Created by varun on 3/8/17.
  */
import scala.util.control._

class prime(val num:Int) {
  val loop = new Breaks

  def findprime(): Unit = {
    loop.breakable {
      for (ele <- 2 to num - 1 by 1) {
        if (num % ele == 0) {
          println("Given No. is not prime")
          loop.break
        }

        else if(num%ele!=0){
          println("It is prime No. and divisible by" + ele)
          loop.break
        }
      }
    }
  }
}


object Prime{
  def main(args:Array[String]): Unit = {
    val input = scala.io.StdIn.readLine()
    val  d = new prime(input.toInt)
    d.findprime()
  }
}