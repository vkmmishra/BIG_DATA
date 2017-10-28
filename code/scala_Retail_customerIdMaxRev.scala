package Retail

/**
  * Created by varun on 10/9/17.
  */
import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SQLContext

object customerIdMaxRev {
  def main(args:Array[String]): Unit ={
    val appProps =  ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Customer Id With Max Rev").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)
    val inPath = new Path(inputPath)
    val outpath = new Path(outputPath + "customerIdMaxRev" )

    if(!fs.exists(inPath))
      println("Input Path does not exists")
    else{
      if(fs.exists(outpath))
        fs.delete(outpath)

      val orders = sc.textFile(inputPath + "orders").map(rec=>{
        val r = rec.split(",")
        Orders(r(0).toInt,r(1).toString,r(2).toInt,r(3).toString)
      }).map(rec=>(rec.orderId,rec.orderCustomerId))
      val orderItems = sc.textFile(inputPath + "order_items").map(rec=>{
        val r = rec.split(",")
        OrderItems(r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)
      }).map(rec=>(rec.orderItemOrderId,rec.orderItemSubtotal))

      val customers = sc.textFile(inputPath + "customers").map(rec=>{
        val r = rec.split(",")
        Customer(r(0).toInt,r(1).toString,r(2).toString, r(3).toString,
          r(4).toString,r(5).toString,r(6).toString,r(7).toString,r(8).toString)
      }).map(rec=>(rec.customerId,rec))

      val ordersJoinItems = orders.join(orderItems).map(rec=>(rec._2._1,rec._2._2))
      val ordersJoinItemsJoinCustomers = ordersJoinItems.join(customers).
        map(rec=>(rec._1,rec._2._1.toFloat)).
        reduceByKey((rec,ele)=>rec+ele)

      val customerIdMaxRevn = sc.parallelize(ordersJoinItemsJoinCustomers.join(customers).
        sortBy(rec=> -rec._2._1).map(rec=>rec._2).take(1))

      customerIdMaxRevn.saveAsTextFile(outputPath + "customerIdMaxRev")

    }





  }

}
