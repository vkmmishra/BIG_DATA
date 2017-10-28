package basics

/**
  * Created by varun on 3/8/17.
  */
class fraction(val n:Int,val d:Int) {
  override def toString = n+"/"+d

 def add(f:fraction) ={
   val num = ((f.d*n) + (f.n*d))
     val denom = (f.d*d)
   new fraction(num,denom)

 }

}

object fraction{
  def main(args:Array[String]): Unit ={
    val f1 = new fraction(1,6)
    println(f1)
    val f2 = new fraction(5,7)
    println(f2)
    val f3 = f1 add f2
    println(f3)


  }
}
