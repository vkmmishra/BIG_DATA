package basics

/**
  * Created by varun on 31/7/17.
  */

object combination {

  def comb(n:Int,r:Int) = {
    def fact(i:Int) = {
      var res = 1
      for(ele<- i to 1 by -1)
        res = res*ele
      res
    }
    fact(n)/(fact(n-r)*fact(r))
  }

  def main(args: Array[String]): Unit = {
print (comb(args(0).toInt,args(1).toInt))
  }

}
