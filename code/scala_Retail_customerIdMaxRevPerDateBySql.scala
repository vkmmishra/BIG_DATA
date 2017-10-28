package Retail

/**
  * Created by varun on 11/9/17.
  */
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config._
import org.apache.spark.sql.SQLContext
object customerIdMaxRevPerDateBySql {
  def main(args:Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Customer Id Max Rev Per Date").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "customerIdMaxRevPerDateBySql")
    if(!fs.exists(inPath))
      println("Input Path Does not Exists")
    else{
      if(fs.exists(outPath))
        fs.delete(outPath)
      val orders = sc.textFile(inputPath + "orders").map(rec=>{
        val r = rec.split(",")
        Orders(r(0).toInt,r(1).toString,r(2).toInt,r(3).toString)
      }).toDF

      orders.registerTempTable("orders")

      val orderItems = sc.textFile(inputPath + "order_items").map(rec=>{
        val r = rec.split(",")
        OrderItems(r(0).toInt,r(1).toInt,r(2).toInt,r(3).toInt,r(4).toFloat,r(5).toFloat)
      }).toDF

      orderItems.registerTempTable("order_items")

      val customers = sc.textFile(inputPath + "customers").map(rec=>{
        val r = rec.split(",")
        Customer(r(0).toInt,r(1).toString,r(2).toString, r(3).toString,
          r(4).toString,r(5).toString,r(6).toString,r(7).toString,r(8).toString)
      }).toDF

      customers.registerTempTable("customers")
      sqlContext.setConf("spark.sql.shuffle.partitions","5")

      val customerIdMaxRevPerDate = sqlContext.sql(

        "select b.customerId,b.orderDate,b.sumTot from (select c.customerId,o.orderDate," +
          " sum(oi.orderItemSubtotal) sumTot from orders o inner join order_items oi on" +
          " o.orderId=oi.orderItemOrderId inner join " +
          " customers c on c.customerId=o.orderCustomerId group by c.customerId,o.orderDate " +
          " order by o.orderDate, sumTot desc)b " +
          " inner join " +
          " (select a.orderDate,max(a.sumTot) maxTot from (select c.customerId,o.orderDate," +
          " sum(oi.orderItemSubtotal) sumTot from orders o inner join order_items " +
          " oi on o.orderId=oi.orderItemOrderId inner join customers c on " +
          " c.customerId=o.orderCustomerId group by c.customerId,o.orderDate " +
          " order by o.orderDate, sumTot desc)a  group by a.orderDate order by " +
          " a.orderDate,maxTot desc) c on b.orderDate=c.orderDate and " +
          " b.sumTot=c.maxTot order by b.orderDate desc ")


      customerIdMaxRevPerDate.take(400).foreach(println)

    }



  }

}
