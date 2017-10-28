package basics

/**
  * Created by varun on 7/10/17.
  */
object Calculator {
  def main(args:Array[String]): Unit ={

    println(addition(5,4))
    println(multiply(5,4))
    println(subtract(10,2))
    println(divide(10,2))

  }

  def addition(x:Int,y:Int) = x+y
  def multiply(a:Int,b:Int) = a*b
  def subtract(n:Int,m:Int) = n-m
  def divide(j:Int,k:Int) = j/k

}
