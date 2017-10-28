package basics

/**
  * Created by varun on 2/8/17.
  */

class Reverseno(var num:String) {
  //import scala.math
  //def rev(): Int ={


  def rev(): Unit ={
    val len = num.length()
    val n = num.toInt
    var res = 0
    for(ele<- len to 1 by -1){
      var rem = (num.toInt)/10
      var mod = (num.toInt)%10
       //res = res + (mod*ele*"0"+""*10)
      num = rem.toString
      //res
      println(res,ele,num,rem,mod)
    }


  }
  //}

}
object reverse{
  def main(args:Array[String]): Unit ={
 val d = new Reverseno("34596")
    d.rev()
  }
}
