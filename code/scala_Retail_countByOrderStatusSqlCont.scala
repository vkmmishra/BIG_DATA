package Retail

/**
  * Created by varun on 29/8/17.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._


/*
case class Orders(orderId:Int,
                  orderDate:String,
                  orderCustomerId:Int,
                  orderStatus:String
                 )

case class OrderItems(orderItemId:Int,
                      orderItemOrderId:Int,
                      orderItemProductId:Int,
                      orderItemQuantity:Int,
                      orderItemSubtotal:Float,
                      orderItemProductPrice:Float
                     )*/

object countByOrderStatusSqlCont {
  def main(args:Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setMaster(appProps.getString("executionMode")).
      setAppName("topNPricedProductByCategory").
      set("spark.ui.port","12349")

    //println("this is conf file " + conf.get
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val inputPath = args(1)
    val outputPath = args(2)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath+"countByOrderStatusSqlCont")

    if(!fs.exists(inPath))
      print("oooooooooooooopppppppppppppp" +
        "sssssssssssssssssss input Path does not exists",inputPath)
    else {
      if(fs.exists(outPath))
        fs.delete(outPath)
      val orders = sc.textFile(inputPath + "orders").map(rec => {
        val r = rec.split(",")
        Orders(r(0).toInt, r(1).toString, r(2).toInt, r(3).toString)
      }).toDF
      orders.registerTempTable("orders")

      val orderItems = sc.textFile(inputPath + "order_items").map(rec => {
        val r = rec.split(",")
        OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toFloat, r(5).toFloat)
      }).toDF
      orderItems.registerTempTable("orderItems")
      sqlContext.setConf("spark.sql.shuffle.partitions","1")

      val ordersJoinItems = sqlContext.sql("select o.orderStatus,count(o.orderStatus) from orders o " +
        "group by o.orderStatus")



      ordersJoinItems.rdd.saveAsTextFile(outputPath + "countByOrderStatusSqlCont")


    }

  }

}
