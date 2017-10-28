package Retail

/**
  * Created by varun on 10/9/17.
  */
import org.apache.spark.{SparkContext,SparkConf}
import com.typesafe.config._
import org.apache.hadoop.fs._

object topNProductByCategory {
  def main(args: Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().
      setAppName("Top N Product By category").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "topNProductByCategory")

    if(!fs.exists(inPath))
      println("Input Path does not exists")
    else{
      if(fs.exists(outPath))
        fs.delete(outPath)

      val products = sc.textFile(inputPath + "products").
        filter(rec=>rec.split(",")(0).toInt!=685)
      val productsMap = products.groupBy(rec=>rec.split(",")(1)).
        flatMap(rec=>rec._2.toList.sortBy(rec=> -rec.split(",")(4).toFloat).take(2)).
        sortBy(rec=> (rec.split(",")(1).toInt,rec.split(",")(0)))

      productsMap.saveAsTextFile(outputPath+"topNProductByCategory")
    }

  }

}
