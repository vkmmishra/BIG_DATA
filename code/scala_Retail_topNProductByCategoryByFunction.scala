package Retail

/**
  * Created by varun on 10/9/17.
  */
import org.apache.spark.{SparkContext,SparkConf}
import com.typesafe.config._
import org.apache.hadoop.fs._



object topNProductByCategoryByFunction {
  def main(args:Array[String]): Unit ={
    val appProps = ConfigFactory.load().getConfig(args(0))
    val conf = new SparkConf().setAppName("Top N Product By Category By Function").
      setMaster(appProps.getString("executionMode")).
      set("spark.ui.port","12349")

    val sc = new SparkContext(conf)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputPath = args(1)
    val outputPath = args(2)
    val inPath = new Path(inputPath)
    val outPath = new Path(outputPath + "topNProductByCategoryByFunction")

    if(!fs.exists(inPath))
      println("Input Path does not exists")
    else{
      if(fs.exists(outPath))
        fs.delete(outPath)

      def topNProduct(ele:(String, Iterable[String]),topN:Int):Iterable[String] = {
      var prodPrices: List[Float] = List()
        var topNPrices: List[Float] = List()
        var sortedRecs: List[String] = List()

        for(i <- ele._2){
          prodPrices = prodPrices:+ i.split(",")(4).toFloat
        }
        topNPrices = prodPrices.distinct.sortBy(x=> -x).take(topN)
        sortedRecs = ele._2.toList.sortBy(rec=> -rec.split(",")(4).toFloat)
        var x: List[String] = List()
          for(i <- sortedRecs){
            if(topNPrices.contains(i.split(",")(4).toFloat))
              x = x:+ i

          }
        return x
      }

      val productsMap = sc.textFile(inputPath + "products").
        filter(rec=>rec.split(",")(0).toInt!=685).groupBy(rec=>rec.split(",")(1)).
        flatMap(rec=>topNProduct(rec,args(3).toInt))

      productsMap.saveAsTextFile(outputPath + "topNProductByCategoryByFunction")
    }


  }

}
