package basics

/**
  * Created by varun on 2/8/17.
  */

class Areacircle(val r:Double = 0.1) {
  println(r)
  def area(radius:Double): Double ={
    val a = 3.14 * radius * radius
    a
  }

}

object Area{
  def main(args: Array[String]): Unit = {
    val ar = new Areacircle()
    println(ar.area(2.0))

  }
}
