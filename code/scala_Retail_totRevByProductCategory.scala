package Retail

/**
  * Created by varun on 31/8/17.
  */


import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.functions._


object totRevByProductCategory {
  def main(args:Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Total Rev By Product Categories").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")
    val sc = new SparkContext(conf)
    val inputPath = args(1)
    val outputPath = args(2)

    val fs =  FileSystem.get(sc.hadoopConfiguration)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath+"totAvgRevByCategory")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if(!fs.exists(inPath))
      println("The Given Input Path Does not exists")
    else
      {
        if(fs.exists(outPath))
          fs.delete(outPath)

        val orders = sc.textFile(inputPath + "orders").map(rec=> {
          val r = rec.split(",")
          Orders(r(0).toInt, r(1).toString, r(2).toInt, r(3).toString )
        })

        val orderItemsMap = sc.textFile(inputPath + "order_items").map(rec => {
          val r = rec.split(",")
          OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toFloat, r(5).toFloat)
        }).map(rec=> (rec.orderItemProductId,(rec.orderItemSubtotal)))

        val productsMap = sc.textFile(inputPath + "products").
          filter(rec=>rec.split(",")(0).toInt!=685).map(rec=> {
          val r = rec.split(",")
          Product(r(0).toInt, r(1).toInt, r(2).toString, r(3).toString,
            r(4).toFloat, r(5).toString)}).
          map(rec=>(rec.productId,(rec.productCategoryId,rec.productName)))

        val categories = sc.textFile(inputPath + "categories").map(f = rec => {
          val r = rec.split(",")
          Category(r(0).toInt, r(1).toInt, r(2).toString)
        }).map(rec=>(rec.categoryId,rec.categoryName))
        val orderItemsMapJoinProductsMap = orderItemsMap.join(productsMap).
          map(rec=>(rec._2._2._1,(rec._2._1)))

        val orderItemsJoinProductsJoinCategories = orderItemsMapJoinProductsMap.
          join(categories).map(rec=>(rec._1,rec._2._1))


        val totRevByCategory = orderItemsJoinProductsJoinCategories.
          aggregateByKey((0.0,0))((rec,ele)=>(rec._1+ele,rec._2+1),
            (rec1,rec2)=>(rec1._1+rec2._1,rec1._2+rec2._2)).
          map(rec=>(rec._1+"\t"+rec._2._1+"\t"+rec._2._2+"\t"+(rec._2._1/rec._2._2)))


        totRevByCategory.saveAsTextFile(outputPath+"totAvgRevByCategory")

        //totRevByCategory.write.json(outputPath+"totAvgRevByCategoryJson")





      }
  }


}
